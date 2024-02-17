<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Projection\Projection\Store;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Types;
use Patchlevel\EventSourcing\Projection\Projection\Projection;
use Patchlevel\EventSourcing\Projection\Projection\ProjectionCollection;
use Patchlevel\EventSourcing\Projection\Projection\ProjectionError;
use Patchlevel\EventSourcing\Projection\Projection\ProjectionNotFound;
use Patchlevel\EventSourcing\Projection\Projection\ProjectionStatus;
use Patchlevel\EventSourcing\Schema\SchemaConfigurator;

use function array_map;
use function json_decode;
use function json_encode;

use const JSON_THROW_ON_ERROR;

/** @psalm-type Data = array{
 *     id: string,
 *     position: int,
 *     status: string,
 *     error_message: string|null,
 *     error_context: string|null,
 *     retry: int,
 * }
 */
final class DoctrineStore implements ProjectionStore, SchemaConfigurator
{
    public function __construct(
        private readonly Connection $connection,
        private readonly string $projectionTable = 'projections',
    ) {
    }

    public function get(string $projectionId): Projection
    {
        $sql = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->projectionTable)
            ->where('id = :id')
            ->getSQL();

        /** @var Data|false $result */
        $result = $this->connection->fetchAssociative($sql, ['id' => $projectionId]);

        if ($result === false) {
            throw new ProjectionNotFound($projectionId);
        }

        return $this->createProjection($result);
    }

    public function all(): ProjectionCollection
    {
        $sql = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->projectionTable)
            ->getSQL();

        /** @var list<Data> $result */
        $result = $this->connection->fetchAllAssociative($sql);

        return new ProjectionCollection(
            array_map(
                fn (array $data) => $this->createProjection($data),
                $result,
            ),
        );
    }

    /** @param Data $row */
    private function createProjection(array $row): Projection
    {
        $context = $row['error_context'] !== null ?
            json_decode($row['error_context'], true, 512, JSON_THROW_ON_ERROR) : null;

        return new Projection(
            $row['id'],
            ProjectionStatus::from($row['status']),
            $row['position'],
            $row['error_message'] !== null ? new ProjectionError(
                $row['error_message'],
                $context,
            ) : null,
            $row['retry'],
        );
    }

    public function save(Projection ...$projections): void
    {
        $this->connection->transactional(
            function (Connection $connection) use ($projections): void {
                foreach ($projections as $projection) {
                    $projectionError = $projection->projectionError();

                    try {
                        $effectedRows = (int)$connection->update(
                            $this->projectionTable,
                            [
                                'position' => $projection->position(),
                                'status' => $projection->status()->value,
                                'error_message' => $projectionError?->errorMessage,
                                'error_context' => $projectionError?->errorContext !== null ? json_encode($projectionError->errorContext, JSON_THROW_ON_ERROR) : null,
                                'retry' => $projection->retry(),
                            ],
                            [
                                'id' => $projection->id(),
                            ],
                        );

                        if ($effectedRows === 0) {
                            $this->get($projection->id()); // check if projection exists, otherwise throw ProjectionNotFound
                        }
                    } catch (ProjectionNotFound) {
                        $connection->insert(
                            $this->projectionTable,
                            [
                                'id' => $projection->id(),
                                'position' => $projection->position(),
                                'status' => $projection->status()->value,
                                'error_message' => $projectionError?->errorMessage,
                                'error_context' => $projectionError?->errorContext !== null ? json_encode($projectionError->errorContext, JSON_THROW_ON_ERROR) : null,
                                'retry' => $projection->retry(),
                            ],
                        );
                    }
                }
            },
        );
    }

    public function remove(string ...$projectionIds): void
    {
        $this->connection->transactional(
            function (Connection $connection) use ($projectionIds): void {
                foreach ($projectionIds as $projectionId) {
                    $connection->delete($this->projectionTable, ['id' => $projectionId]);
                }
            },
        );
    }

    public function configureSchema(Schema $schema, Connection $connection): void
    {
        $table = $schema->createTable($this->projectionTable);

        $table->addColumn('id', Types::STRING)
            ->setLength(255)
            ->setNotnull(true);
        $table->addColumn('position', Types::INTEGER)
            ->setNotnull(true);
        $table->addColumn('status', Types::STRING)
            ->setLength(32)
            ->setNotnull(true);
        $table->addColumn('error_message', Types::STRING)
            ->setLength(255)
            ->setNotnull(false);
        $table->addColumn('error_context', Types::JSON)
            ->setNotnull(false);
        $table->addColumn('retry', Types::INTEGER)
            ->setNotnull(true)
            ->setDefault(0);

        $table->setPrimaryKey(['id']);
    }
}
