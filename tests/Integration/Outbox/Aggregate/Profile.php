<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Integration\Outbox\Aggregate;

use Patchlevel\EventSourcing\Aggregate\BasicAggregateRoot;
use Patchlevel\EventSourcing\Attribute\Aggregate;
use Patchlevel\EventSourcing\Attribute\AggregateId;
use Patchlevel\EventSourcing\Attribute\Apply;
use Patchlevel\EventSourcing\Attribute\Snapshot;
use Patchlevel\EventSourcing\Tests\Integration\Outbox\Events\ProfileCreated;
use Patchlevel\EventSourcing\Tests\Integration\Outbox\Normalizer\ProfileIdNormalizer;
use Patchlevel\EventSourcing\Tests\Integration\Outbox\ProfileId;

#[Aggregate('profile')]
#[Snapshot('default', 100)]
final class Profile extends BasicAggregateRoot
{
    #[AggregateId]
    #[ProfileIdNormalizer]
    private ProfileId $id;
    private string $name;

    public static function create(ProfileId $id, string $name): self
    {
        $self = new self();
        $self->recordThat(new ProfileCreated($id, $name));

        return $self;
    }

    #[Apply(ProfileCreated::class)]
    protected function applyProfileCreated(ProfileCreated $event): void
    {
        $this->id = $event->profileId;
        $this->name = $event->name;
    }

    public function name(): string
    {
        return $this->name;
    }
}
