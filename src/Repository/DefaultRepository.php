<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Repository;

use Patchlevel\EventSourcing\Aggregate\AggregateRoot;
use Patchlevel\EventSourcing\Clock\Clock;
use Patchlevel\EventSourcing\Clock\SystemClock;
use Patchlevel\EventSourcing\EventBus\Decorator\MessageDecorator;
use Patchlevel\EventSourcing\EventBus\EventBus;
use Patchlevel\EventSourcing\EventBus\Message;
use Patchlevel\EventSourcing\Metadata\AggregateRoot\AggregateRootMetadata;
use Patchlevel\EventSourcing\Snapshot\SnapshotNotFound;
use Patchlevel\EventSourcing\Snapshot\SnapshotStore;
use Patchlevel\EventSourcing\Snapshot\SnapshotVersionInvalid;
use Patchlevel\EventSourcing\Store\ArchivableStore;
use Patchlevel\EventSourcing\Store\CriteriaBuilder;
use Patchlevel\EventSourcing\Store\Store;
use Patchlevel\EventSourcing\Store\Stream;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Throwable;
use Traversable;
use WeakMap;

use function array_map;
use function assert;
use function count;
use function sprintf;

/**
 * @template T of AggregateRoot
 * @implements Repository<T>
 */
final class DefaultRepository implements Repository
{
    private Clock $clock;
    private LoggerInterface $logger;

    /** @var WeakMap<T, bool> */
    private WeakMap $aggregateIsValid;

    /** @param AggregateRootMetadata<T> $metadata */
    public function __construct(
        private Store $store,
        private EventBus $eventBus,
        private readonly AggregateRootMetadata $metadata,
        private SnapshotStore|null $snapshotStore = null,
        private MessageDecorator|null $messageDecorator = null,
        Clock|null $clock = null,
        LoggerInterface|null $logger = null,
    ) {
        $this->clock = $clock ?? new SystemClock();
        $this->logger = $logger ?? new NullLogger();
        $this->aggregateIsValid = new WeakMap();
    }

    /** @return T */
    public function load(string $id): AggregateRoot
    {
        if ($this->snapshotStore && $this->metadata->snapshot) {
            try {
                return $this->loadFromSnapshot($this->metadata->className, $id);
            } catch (SnapshotRebuildFailed $exception) {
                $this->logger->error($exception->getMessage());
            } catch (SnapshotNotFound) {
                $this->logger->debug(
                    sprintf(
                        'snapshot for aggregate "%s" with the id "%s" not found',
                        $this->metadata->className,
                        $id,
                    ),
                );
            } catch (SnapshotVersionInvalid) {
                $this->logger->debug(
                    sprintf(
                        'snapshot for aggregate "%s" with the id "%s" is invalid',
                        $this->metadata->className,
                        $id,
                    ),
                );
            }
        }

        $criteria = (new CriteriaBuilder())
            ->aggregateClass($this->metadata->className)
            ->aggregateId($id)
            ->archived(false)
            ->build();

        $stream = $this->store->load($criteria);

        $firstMessage = $stream->current();

        if ($firstMessage === null) {
            throw new AggregateNotFound($this->metadata->className, $id);
        }

        $aggregate = $this->metadata->className::createFromEvents(
            $this->unpack($stream),
            $firstMessage->playhead() - 1,
        );

        if ($this->snapshotStore && $this->metadata->snapshot) {
            $this->saveSnapshot($aggregate, $stream);
        }

        $this->aggregateIsValid[$aggregate] = true;

        return $aggregate;
    }

    public function has(string $id): bool
    {
        $criteria = (new CriteriaBuilder())
            ->aggregateClass($this->metadata->className)
            ->aggregateId($id)
            ->build();

        return $this->store->count($criteria) > 0;
    }

    /** @param T $aggregate */
    public function save(AggregateRoot $aggregate): void
    {
        $this->assertValidAggregate($aggregate);

        try {
            $events = $aggregate->releaseEvents();
            $eventCount = count($events);

            if ($eventCount === 0) {
                return;
            }

            $playhead = $aggregate->playhead() - $eventCount;

            if (!isset($this->aggregateIsValid[$aggregate]) && $playhead !== 0) {
                throw new AggregateUnknown($aggregate::class, $aggregate->aggregateRootId());
            }

            if ($playhead < 0) {
                throw new PlayheadMismatch(
                    $aggregate::class,
                    $aggregate->aggregateRootId(),
                    $aggregate->playhead(),
                    $eventCount,
                );
            }

            $messageDecorator = $this->messageDecorator;
            $clock = $this->clock;

            $messages = array_map(
                static function (object $event) use ($aggregate, &$playhead, $messageDecorator, $clock) {
                    $message = Message::create($event)
                        ->withAggregateClass($aggregate::class)
                        ->withAggregateId($aggregate->aggregateRootId())
                        ->withPlayhead(++$playhead)
                        ->withRecordedOn($clock->now());

                    if ($messageDecorator) {
                        return $messageDecorator($message);
                    }

                    return $message;
                },
                $events,
            );

            $this->store->transactional(function () use ($messages): void {
                $this->store->save(...$messages);
                $this->archive(...$messages);
                $this->eventBus->dispatch(...$messages);
            });

            $this->aggregateIsValid[$aggregate] = true;
        } catch (Throwable $exception) {
            $this->aggregateIsValid[$aggregate] = false;

            throw $exception;
        }
    }

    /**
     * @param class-string<T> $aggregateClass
     *
     * @return T
     */
    private function loadFromSnapshot(string $aggregateClass, string $id): AggregateRoot
    {
        assert($this->snapshotStore instanceof SnapshotStore);

        $aggregate = $this->snapshotStore->load($aggregateClass, $id);

        $criteria = (new CriteriaBuilder())
            ->aggregateClass($this->metadata->className)
            ->aggregateId($id)
            ->fromPlayhead($aggregate->playhead())
            ->build();

        $stream = $this->store->load($criteria);

        if ($stream->current() === null) {
            $this->aggregateIsValid[$aggregate] = true;

            return $aggregate;
        }

        try {
            $aggregate->catchUp($this->unpack($stream));
        } catch (Throwable $exception) {
            throw new SnapshotRebuildFailed($aggregateClass, $id, $exception);
        }

        $this->saveSnapshot($aggregate, $stream);

        $this->aggregateIsValid[$aggregate] = true;

        return $aggregate;
    }

    /** @param T $aggregate */
    private function saveSnapshot(AggregateRoot $aggregate, Stream $stream): void
    {
        assert($this->snapshotStore instanceof SnapshotStore);

        $batchSize = $this->metadata->snapshot?->batch ?: 1;

        if ($stream->position() < $batchSize) {
            return;
        }

        $this->snapshotStore->save($aggregate);
    }

    private function assertValidAggregate(AggregateRoot $aggregate): void
    {
        if (!$aggregate instanceof $this->metadata->className) {
            throw new WrongAggregate($aggregate::class, $this->metadata->className);
        }

        if (($this->aggregateIsValid[$aggregate] ?? null) === false) {
            throw new AggregateDetached($aggregate::class, $aggregate->aggregateRootId());
        }
    }

    private function archive(Message ...$messages): void
    {
        if (!$this->store instanceof ArchivableStore) {
            return;
        }

        foreach ($messages as $message) {
            if (!$message->newStreamStart()) {
                continue;
            }

            $this->store->archiveMessages(
                $message->aggregateClass(),
                $message->aggregateId(),
                $message->playhead(),
            );
        }
    }

    /** @return Traversable<object> */
    private function unpack(Stream $stream): Traversable
    {
        foreach ($stream as $message) {
            yield $message->event();
        }
    }
}
