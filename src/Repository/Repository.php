<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Repository;

use Patchlevel\EventSourcing\Aggregate\AggregateRoot;
use Patchlevel\EventSourcing\Aggregate\SnapshotableAggregateRoot;
use Patchlevel\EventSourcing\EventBus\EventBus;
use Patchlevel\EventSourcing\Snapshot\SnapshotNotFound;
use Patchlevel\EventSourcing\Snapshot\SnapshotStore;
use Patchlevel\EventSourcing\Store\Store;

use function array_key_exists;
use function count;
use function get_class;
use function is_subclass_of;

final class Repository
{
    private Store $store;
    private EventBus $eventStream;

    /** @var class-string<AggregateRoot> */
    private string $aggregateClass;

    /** @var array<string, AggregateRoot> */
    private array $instances = [];

    private ?SnapshotStore $snapshotStore;

    /**
     * @param class-string $aggregateClass
     */
    public function __construct(
        Store $store,
        EventBus $eventStream,
        string $aggregateClass,
        ?SnapshotStore $snapshotStore = null
    ) {
        if (!is_subclass_of($aggregateClass, AggregateRoot::class)) {
            throw InvalidAggregateClass::notAggregateRoot($aggregateClass);
        }

        if ($snapshotStore && !is_subclass_of($aggregateClass, SnapshotableAggregateRoot::class)) {
            throw InvalidAggregateClass::notSnapshotableAggregateRoot($aggregateClass);
        }

        $this->store = $store;
        $this->eventStream = $eventStream;
        $this->aggregateClass = $aggregateClass;
        $this->snapshotStore = $snapshotStore;
    }

    public function load(string $id): AggregateRoot
    {
        if (array_key_exists($id, $this->instances)) {
            return $this->instances[$id];
        }

        $aggregateClass = $this->aggregateClass;

        if ($this->snapshotStore) {
            if (!is_subclass_of($aggregateClass, SnapshotableAggregateRoot::class)) {
                throw InvalidAggregateClass::notSnapshotableAggregateRoot($aggregateClass);
            }

            try {
                $snapshot = $this->snapshotStore->load($aggregateClass, $id);

                $events = $this->store->load($this->aggregateClass, $id, $snapshot->playhead());

                $instance = $aggregateClass::createFromSnapshot(
                    $snapshot,
                    $events
                );

                return $this->instances[$id] = $instance;
            } catch (SnapshotNotFound $exception) {
                // do normal workflow
            }
        }

        $events = $this->store->load($this->aggregateClass, $id);

        if (count($events) === 0) {
            throw new AggregateNotFound($this->aggregateClass, $id);
        }

        return $this->instances[$id] = $this->aggregateClass::createFromEventStream($events);
    }

    public function has(string $id): bool
    {
        if (array_key_exists($id, $this->instances)) {
            return true;
        }

        return $this->store->has($this->aggregateClass, $id);
    }

    public function save(AggregateRoot $aggregate): void
    {
        $class = get_class($aggregate);

        if (!$aggregate instanceof $this->aggregateClass) {
            throw new WrongAggregate($class, $this->aggregateClass);
        }

        $eventStream = $aggregate->releaseEvents();

        if (count($eventStream) === 0) {
            return;
        }

        $this->store->saveBatch($this->aggregateClass, $aggregate->aggregateRootId(), $eventStream);

        if ($this->snapshotStore) {
            if (!$aggregate instanceof SnapshotableAggregateRoot) {
                throw InvalidAggregateClass::notSnapshotableAggregateRoot($class);
            }

            $snapshot = $aggregate->toSnapshot();
            $this->snapshotStore->save($snapshot);
        }

        foreach ($eventStream as $event) {
            $this->eventStream->dispatch($event);
        }
    }
}
