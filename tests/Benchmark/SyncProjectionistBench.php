<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Benchmark;

use Doctrine\DBAL\Driver\PDO\SQLite\Driver;
use Doctrine\DBAL\DriverManager;
use Patchlevel\EventSourcing\EventBus\ChainEventBus;
use Patchlevel\EventSourcing\EventBus\DefaultEventBus;
use Patchlevel\EventSourcing\EventBus\EventBus;
use Patchlevel\EventSourcing\Lock\DoctrineDbalStoreSchemaAdapter;
use Patchlevel\EventSourcing\Projection\Projection\Store\InMemoryStore;
use Patchlevel\EventSourcing\Projection\Projectionist\DefaultProjectionist;
use Patchlevel\EventSourcing\Projection\Projectionist\ProjectionistEventBusMiddleware;
use Patchlevel\EventSourcing\Projection\Projector\InMemoryProjectorRepository;
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
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\Store\DoctrineDbalStore as LockDoctrineDbalStore;

use function file_exists;
use function unlink;

#[Bench\BeforeMethods('setUp')]
final class SyncProjectionistBench
{
    private const DB_PATH = __DIR__ . '/BasicImplementation/data/db.sqlite3';

    private Store $store;
    private EventBus $bus;
    private Repository $repository;
    private Profile $profile;

    private ProfileId $id;

    public function setUp(): void
    {
        if (file_exists(self::DB_PATH)) {
            unlink(self::DB_PATH);
        }

        $connection = DriverManager::getConnection([
            'driverClass' => Driver::class,
            'path' => self::DB_PATH,
        ]);

        $this->store = new DoctrineDbalStore(
            $connection,
            DefaultEventSerializer::createFromPaths([__DIR__ . '/BasicImplementation/Events']),
            'eventstore',
        );

        $profileProjection = new ProfileProjector($connection);
        $projectionRepository = new InMemoryProjectorRepository(
            [$profileProjection],
        );

        $projectionist = new DefaultProjectionist(
            $this->store,
            new InMemoryStore(),
            $projectionRepository,
        );

        $lockStorage = new LockDoctrineDbalStore($connection);
        $lockStorageAdapter = new DoctrineDbalStoreSchemaAdapter($lockStorage);

        $this->bus = new ChainEventBus([
            DefaultEventBus::create([new SendEmailProcessor()]),
            new ProjectionistEventBusMiddleware(
                $projectionist,
                new LockFactory(
                    new LockDoctrineDbalStore($connection),
                ),
            ),
        ]);

        $this->repository = new DefaultRepository($this->store, $this->bus, Profile::metadata());

        $schemaDirector = new DoctrineSchemaDirector(
            $connection,
            new ChainSchemaConfigurator(
                [
                    $this->store,
                    $lockStorageAdapter,
                ],
            ),
        );

        $schemaDirector->create();
        $projectionist->boot();

        $this->id = ProfileId::v7();
        $this->profile = Profile::create($this->id, 'Peter');
        $this->repository->save($this->profile);
    }

    #[Bench\Revs(20)]
    public function benchSave1Event(): void
    {
        $this->profile->changeName('Peter');
        $this->repository->save($this->profile);
    }

    #[Bench\Revs(20)]
    public function benchSave10000Events(): void
    {
        for ($i = 0; $i < 10_000; $i++) {
            $this->profile->changeName('Peter');
        }

        $this->repository->save($this->profile);
    }
}
