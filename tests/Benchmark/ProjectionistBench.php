<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Benchmark;

use Doctrine\DBAL\Driver\PDO\SQLite\Driver;
use Doctrine\DBAL\DriverManager;
use Patchlevel\EventSourcing\Aggregate\AggregateRootId;
use Patchlevel\EventSourcing\EventBus\DefaultEventBus;
use Patchlevel\EventSourcing\EventBus\EventBus;
use Patchlevel\EventSourcing\Projection\Projection\Store\DoctrineStore;
use Patchlevel\EventSourcing\Projection\Projectionist\DefaultProjectionist;
use Patchlevel\EventSourcing\Projection\Projectionist\Projectionist;
use Patchlevel\EventSourcing\Repository\DefaultRepository;
use Patchlevel\EventSourcing\Repository\Repository;
use Patchlevel\EventSourcing\Schema\ChainSchemaConfigurator;
use Patchlevel\EventSourcing\Schema\DoctrineSchemaDirector;
use Patchlevel\EventSourcing\Serializer\DefaultEventSerializer;
use Patchlevel\EventSourcing\Store\DoctrineDbalStore;
use Patchlevel\EventSourcing\Store\Store;
use Patchlevel\EventSourcing\Tests\Benchmark\BasicImplementation\Aggregate\Profile;
use Patchlevel\EventSourcing\Tests\Benchmark\BasicImplementation\Processor\SendEmailProcessor;
use Patchlevel\EventSourcing\Tests\Benchmark\BasicImplementation\ProfileId;
use Patchlevel\EventSourcing\Tests\Benchmark\BasicImplementation\Projection\ProfileProjector;
use PhpBench\Attributes as Bench;

use function file_exists;
use function unlink;

#[Bench\BeforeMethods('setUp')]
final class ProjectionistBench
{
    private const DB_PATH = __DIR__ . '/BasicImplementation/data/db.sqlite3';

    private Store $store;
    private EventBus $bus;
    private Repository $repository;

    private Projectionist $projectionist;

    private AggregateRootId $id;

    public function setUp(): void
    {
        if (file_exists(self::DB_PATH)) {
            unlink(self::DB_PATH);
        }

        $connection = DriverManager::getConnection([
            'driverClass' => Driver::class,
            'path' => self::DB_PATH,
        ]);

        $this->bus = DefaultEventBus::create();

        $this->store = new DoctrineDbalStore(
            $connection,
            DefaultEventSerializer::createFromPaths([__DIR__ . '/BasicImplementation/Events']),
            'eventstore',
        );

        $this->repository = new DefaultRepository($this->store, $this->bus, Profile::metadata());

        $projectionStore = new DoctrineStore(
            $connection,
        );

        $schemaDirector = new DoctrineSchemaDirector(
            $connection,
            new ChainSchemaConfigurator([
                $this->store,
                $projectionStore,
            ]),
        );

        $schemaDirector->create();

        $this->id = ProfileId::v7();

        $profile = Profile::create($this->id, 'Peter');

        for ($i = 1; $i < 10_000; $i++) {
            $profile->changeName('Peter ' . $i);
        }

        $this->repository->save($profile);

        $this->projectionist = new DefaultProjectionist(
            $this->store,
            $projectionStore,
            [
                new ProfileProjector($connection),
                new SendEmailProcessor(),
            ],
        );
    }

    #[Bench\Revs(20)]
    public function benchHandle10000Events(): void
    {
        $this->projectionist->boot();
        $this->projectionist->remove();
    }
}
