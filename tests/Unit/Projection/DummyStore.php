<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Projection;

use Patchlevel\EventSourcing\Projection\Projection\Projection;
use Patchlevel\EventSourcing\Projection\Projection\ProjectionCollection;
use Patchlevel\EventSourcing\Projection\Projection\ProjectionId;
use Patchlevel\EventSourcing\Projection\Projection\ProjectionNotFound;
use Patchlevel\EventSourcing\Projection\Projection\Store\ProjectionStore;

use function array_key_exists;
use function array_values;

final class DummyStore implements ProjectionStore
{
    /** @var array<string, Projection> */
    private array $store = [];
    /** @var list<Projection> */
    public array $savedStates = [];
    /** @var list<ProjectionId> */
    public array $removedIds = [];

    /**
     * @param list<Projection> $store
     */
    public function __construct(array $store = [])
    {
        foreach ($store as $state) {
            $this->store[$state->id()->toString()] = $state;
        }
    }

    public function get(ProjectionId $projectionId): Projection
    {
        if (array_key_exists($projectionId->toString(), $this->store)) {
            return $this->store[$projectionId->toString()];
        }

        throw new ProjectionNotFound($projectionId);
    }

    public function all(): ProjectionCollection
    {
        return new ProjectionCollection(array_values($this->store));
    }

    public function save(Projection ...$projections): void
    {
        foreach ($projections as $state) {
            $this->store[$state->id()->toString()] = $state;
            $this->savedStates[] = clone $state;
        }
    }

    public function remove(ProjectionId ...$projectionIds): void
    {
        foreach ($projectionIds as $projectionId) {
            $this->removedIds[] = $projectionId;
            unset($this->store[$projectionId->toString()]);
        }
    }
}
