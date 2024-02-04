<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Store;

use Closure;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Patchlevel\EventSourcing\Aggregate\AggregateRoot;
use Patchlevel\EventSourcing\EventBus\HeaderNotFound;
use Patchlevel\EventSourcing\EventBus\Message;
use Patchlevel\EventSourcing\Metadata\AggregateRoot\AggregateRootRegistry;
use Patchlevel\EventSourcing\Schema\SchemaConfigurator;
use Patchlevel\EventSourcing\Serializer\EventSerializer;

use function array_fill;
use function count;
use function implode;
use function is_int;
use function is_string;
use function sprintf;

final class DoctrineDbalStore implements Store, ArchivableStore, SchemaConfigurator
{
    public function __construct(
        private readonly Connection $connection,
        private readonly EventSerializer $serializer,
        private readonly AggregateRootRegistry $aggregateRootRegistry,
        private readonly string $storeTableName = 'eventstore',
    ) {
    }

    public function load(
        Criteria|null $criteria = null,
        int|null $limit = null,
        int|null $offset = null,
        bool $backwards = false,
    ): DoctrineDbalStoreStream {
        $builder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->storeTableName)
            ->orderBy('id', $backwards ? 'DESC' : 'ASC');

        $this->applyCriteria($builder, $criteria ?? new Criteria());

        $builder->setMaxResults($limit);
        $builder->setFirstResult($offset ?? 0);

        return new DoctrineDbalStoreStream(
            $this->connection->executeQuery(
                $builder->getSQL(),
                $builder->getParameters(),
                $builder->getParameterTypes(),
            ),
            $this->serializer,
            $this->aggregateRootRegistry,
            $this->connection->getDatabasePlatform(),
        );
    }

    public function count(Criteria|null $criteria = null): int
    {
        $builder = $this->connection->createQueryBuilder()
            ->select('COUNT(*)')
            ->from($this->storeTableName);

        $this->applyCriteria($builder, $criteria ?? new Criteria());

        $result = $this->connection->fetchOne(
            $builder->getSQL(),
            $builder->getParameters(),
            $builder->getParameterTypes(),
        );

        if (!is_int($result) && !is_string($result)) {
            throw new WrongQueryResult();
        }

        return (int)$result;
    }

    private function applyCriteria(QueryBuilder $builder, Criteria $criteria): void
    {
        if ($criteria->aggregateClass !== null) {
            $shortName = $this->aggregateRootRegistry->aggregateName($criteria->aggregateClass);
            $builder->andWhere('aggregate = :aggregate');
            $builder->setParameter('aggregate', $shortName);
        }

        if ($criteria->aggregateId !== null) {
            $builder->andWhere('aggregate_id = :id');
            $builder->setParameter('id', $criteria->aggregateId);
        }

        if ($criteria->fromPlayhead !== null) {
            $builder->andWhere('playhead > :playhead');
            $builder->setParameter('playhead', $criteria->fromPlayhead, Types::INTEGER);
        }

        if ($criteria->archived !== null) {
            $builder->andWhere('archived = :archived');
            $builder->setParameter('archived', $criteria->archived, Types::BOOLEAN);
        }

        if ($criteria->fromIndex === null) {
            return;
        }

        $builder->andWhere('id > :index');
        $builder->setParameter('index', $criteria->fromIndex, Types::INTEGER);
    }

    public function save(Message ...$messages): void
    {
        if ($messages === []) {
            return;
        }

        $this->connection->transactional(
            function (Connection $connection) use ($messages): void {
                $booleanType = Type::getType(Types::BOOLEAN);
                $jsonType = Type::getType(Types::JSON);
                $dateTimeType = Type::getType(Types::DATETIMETZ_IMMUTABLE);

                $parameters = [];
                $placeholders = [];

                /** @var array<int<0, max>, Type> $types */
                $types = [];

                $columns = [
                    'aggregate',
                    'aggregate_id',
                    'playhead',
                    'event',
                    'payload',
                    'recorded_on',
                    'new_stream_start',
                    'archived',
                    'custom_headers',
                ];

                $columnsLength = count($columns);
                $placeholder = implode(', ', array_fill(0, $columnsLength, '?'));

                foreach ($messages as $position => $message) {
                    /** @var int<0, max> $offset */
                    $offset = (int)$position * $columnsLength;
                    $placeholders[] = $placeholder;

                    $data = $this->serializer->serialize($message->event());

                    try {
                        $parameters[] = $this->aggregateRootRegistry->aggregateName($message->aggregateClass());
                        $parameters[] = $message->aggregateId();
                        $parameters[] = $message->playhead();
                        $parameters[] = $data->name;
                        $parameters[] = $data->payload;

                        $parameters[] = $message->recordedOn();
                        $types[$offset + 5] = $dateTimeType;

                        $parameters[] = $message->newStreamStart();
                        $types[$offset + 6] = $booleanType;

                        $parameters[] = $message->archived();
                        $types[$offset + 7] = $booleanType;

                        $parameters[] = $message->customHeaders();
                        $types[$offset + 8] = $jsonType;
                    } catch (HeaderNotFound $e) {
                        throw new MissingDataForStorage($e->name, $e);
                    }
                }

                $query = sprintf(
                    "INSERT INTO %s (%s) VALUES\n(%s)",
                    $this->storeTableName,
                    implode(', ', $columns),
                    implode("),\n(", $placeholders),
                );

                try {
                    $connection->executeStatement($query, $parameters, $types);
                } catch (UniqueConstraintViolationException $e) {
                    throw new UniqueConstraintViolation($e);
                }
            },
        );
    }

    /**
     * @param Closure():ClosureReturn $function
     *
     * @template ClosureReturn
     */
    public function transactional(Closure $function): void
    {
        $this->connection->transactional($function);
    }

    /** @param class-string<AggregateRoot> $aggregate */
    public function archiveMessages(string $aggregate, string $id, int $untilPlayhead): void
    {
        $aggregateName = $this->aggregateRootRegistry->aggregateName($aggregate);

        $statement = $this->connection->prepare(sprintf(
            'UPDATE %s 
            SET archived = true
            WHERE aggregate = :aggregate
            AND aggregate_id = :aggregate_id
            AND playhead < :playhead
            AND archived = false',
            $this->storeTableName,
        ));

        $statement->bindValue('aggregate', $aggregateName);
        $statement->bindValue('aggregate_id', $id);
        $statement->bindValue('playhead', $untilPlayhead);

        $statement->executeQuery();
    }

    public function configureSchema(Schema $schema, Connection $connection): void
    {
        if ($this->connection !== $connection) {
            return;
        }

        $table = $schema->createTable($this->storeTableName);

        $table->addColumn('id', Types::BIGINT)
            ->setAutoincrement(true)
            ->setNotnull(true);
        $table->addColumn('aggregate', Types::STRING)
            ->setNotnull(true);
        $table->addColumn('aggregate_id', Types::STRING)
            ->setNotnull(true);
        $table->addColumn('playhead', Types::INTEGER)
            ->setNotnull(true);
        $table->addColumn('event', Types::STRING)
            ->setNotnull(true);
        $table->addColumn('payload', Types::JSON)
            ->setNotnull(true);
        $table->addColumn('recorded_on', Types::DATETIMETZ_IMMUTABLE)
            ->setNotnull(false);
        $table->addColumn('new_stream_start', Types::BOOLEAN)
            ->setNotnull(true)
            ->setDefault(false);
        $table->addColumn('archived', Types::BOOLEAN)
            ->setNotnull(true)
            ->setDefault(false);
        $table->addColumn('custom_headers', Types::JSON)
            ->setNotnull(true);

        $table->setPrimaryKey(['id']);
        $table->addUniqueIndex(['aggregate', 'aggregate_id', 'playhead']);
        $table->addIndex(['aggregate', 'aggregate_id', 'playhead', 'archived']);
    }
}
