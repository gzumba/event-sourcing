<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Store;

use Closure;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Patchlevel\EventSourcing\Aggregate\AggregateHeader;
use Patchlevel\EventSourcing\Message\HeaderNotFound;
use Patchlevel\EventSourcing\Message\Message;
use Patchlevel\EventSourcing\Message\Serializer\DefaultHeadersSerializer;
use Patchlevel\EventSourcing\Message\Serializer\HeadersSerializer;
use Patchlevel\EventSourcing\Schema\DoctrineSchemaConfigurator;
use Patchlevel\EventSourcing\Serializer\EventSerializer;
use PDO;

use function array_fill;
use function array_filter;
use function array_values;
use function class_exists;
use function count;
use function explode;
use function floor;
use function implode;
use function in_array;
use function is_int;
use function is_string;
use function sprintf;

final class DoctrineDbalStore implements Store, ArchivableStore, SubscriptionStore, DoctrineSchemaConfigurator
{
    /**
     * PostgreSQL has a limit of 65535 parameters in a single query.
     */
    private const MAX_UNSIGNED_SMALL_INT = 65_535;

    private readonly HeadersSerializer $headersSerializer;

    public function __construct(
        private readonly Connection $connection,
        private readonly EventSerializer $eventSerializer,
        HeadersSerializer|null $headersSerializer = null,
        private readonly string $storeTableName = 'eventstore',
    ) {
        $this->headersSerializer = $headersSerializer ?? DefaultHeadersSerializer::createDefault();
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
            $this->eventSerializer,
            $this->headersSerializer,
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
        if ($criteria->aggregateName !== null) {
            $shortName = $criteria->aggregateName;
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
                $dateTimeType = Type::getType(Types::DATETIMETZ_IMMUTABLE);

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
                $batchSize = (int)floor(self::MAX_UNSIGNED_SMALL_INT / $columnsLength);
                $placeholder = implode(', ', array_fill(0, $columnsLength, '?'));

                $parameters = [];
                $placeholders = [];
                /** @var array<int<0, max>, Type> $types */
                $types = [];
                $position = 0;
                foreach ($messages as $message) {
                    /** @var int<0, max> $offset */
                    $offset = $position * $columnsLength;
                    $placeholders[] = $placeholder;

                    $data = $this->eventSerializer->serialize($message->event());

                    try {
                        $aggregateHeader = $message->header(AggregateHeader::class);
                    } catch (HeaderNotFound $e) {
                        throw new MissingDataForStorage($e->name, $e);
                    }

                    $parameters[] = $aggregateHeader->aggregateName;
                    $parameters[] = $aggregateHeader->aggregateId;
                    $parameters[] = $aggregateHeader->playhead;
                    $parameters[] = $data->name;
                    $parameters[] = $data->payload;

                    $parameters[] = $aggregateHeader->recordedOn;
                    $types[$offset + 5] = $dateTimeType;

                    $parameters[] = $message->hasHeader(StreamStartHeader::class);
                    $types[$offset + 6] = $booleanType;

                    $parameters[] = $message->hasHeader(ArchivedHeader::class);
                    $types[$offset + 7] = $booleanType;

                    $parameters[] = $this->headersSerializer->serialize($this->getCustomHeaders($message));

                    $position++;

                    if ($position !== $batchSize) {
                        continue;
                    }

                    $this->executeSave($columns, $placeholders, $parameters, $types, $connection);

                    $parameters = [];
                    $placeholders = [];
                    $types = [];

                    $position = 0;
                }

                if ($position === 0) {
                    return;
                }

                $this->executeSave($columns, $placeholders, $parameters, $types, $connection);
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

    public function archiveMessages(string $aggregateName, string $aggregateId, int $untilPlayhead): void
    {
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
        $statement->bindValue('aggregate_id', $aggregateId);
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
            ->setAutoincrement(true);
        $table->addColumn('aggregate', Types::STRING)
            ->setLength(255)
            ->setNotnull(true);
        $table->addColumn('aggregate_id', Types::STRING)
            ->setLength(36)
            ->setNotnull(true);
        $table->addColumn('playhead', Types::INTEGER)
            ->setNotnull(true);
        $table->addColumn('event', Types::STRING)
            ->setLength(255)
            ->setNotnull(true);
        $table->addColumn('payload', Types::JSON)
            ->setNotnull(true);
        $table->addColumn('recorded_on', Types::DATETIMETZ_IMMUTABLE)
            ->setNotnull(true);
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

    /** @return list<object> */
    private function getCustomHeaders(Message $message): array
    {
        $filteredHeaders = [
            AggregateHeader::class,
            StreamStartHeader::class,
            ArchivedHeader::class,
        ];

        return array_values(
            array_filter(
                $message->headers(),
                static fn (object $header) => !in_array($header::class, $filteredHeaders, true),
            ),
        );
    }

    public function supportSubscription(): bool
    {
        return $this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform && class_exists(PDO::class);
    }

    public function wait(int $timeoutMilliseconds): void
    {
        if (!$this->supportSubscription()) {
            return;
        }

        $this->connection->executeStatement(sprintf('LISTEN "%s"', $this->storeTableName));

        /** @var PDO $nativeConnection */
        $nativeConnection = $this->connection->getNativeConnection();

        $nativeConnection->pgsqlGetNotify(PDO::FETCH_ASSOC, $timeoutMilliseconds);
    }

    public function setupSubscription(): void
    {
        if (!$this->supportSubscription()) {
            return;
        }

        $functionName = $this->createTriggerFunctionName();

        $this->connection->executeStatement(sprintf(
            <<<'SQL'
                CREATE OR REPLACE FUNCTION %1$s() RETURNS TRIGGER AS $$
                    BEGIN
                        PERFORM pg_notify('%2$s', 'update');
                        RETURN NEW;
                    END;
                $$ LANGUAGE plpgsql;
                SQL,
            $functionName,
            $this->storeTableName,
        ));

        $this->connection->executeStatement(sprintf('DROP TRIGGER IF EXISTS notify_trigger ON %s;', $this->storeTableName));
        $this->connection->executeStatement(sprintf('CREATE TRIGGER notify_trigger AFTER INSERT OR UPDATE ON %1$s FOR EACH ROW EXECUTE PROCEDURE %2$s();', $this->storeTableName, $functionName));
    }

    private function createTriggerFunctionName(): string
    {
        $tableConfig = explode('.', $this->storeTableName);

        if (count($tableConfig) === 1) {
            return sprintf('notify_%1$s', $tableConfig[0]);
        }

        return sprintf('%1$s.notify_%2$s', $tableConfig[0], $tableConfig[1]);
    }

    /**
     * @param array<string>               $columns
     * @param array<string>               $placeholders
     * @param list<mixed>                 $parameters
     * @param array<0|positive-int, Type> $types
     */
    private function executeSave(array $columns, array $placeholders, array $parameters, array $types, Connection $connection): void
    {
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
    }
}
