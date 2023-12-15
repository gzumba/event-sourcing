<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Fixture;

use Patchlevel\EventSourcing\Aggregate\BasicAggregateRoot;
use Patchlevel\EventSourcing\Attribute\Aggregate;
use Patchlevel\EventSourcing\Attribute\AggregateId;
use Patchlevel\EventSourcing\Attribute\Apply;

#[Aggregate(ProfileInvalid::class)]
final class ProfileInvalid extends BasicAggregateRoot
{
    #[AggregateId]
    private ProfileId $id;
    private Email $email;

    public static function createProfile(ProfileId $id, Email $email): self
    {
        $self = new self();
        $self->recordThat(new ProfileCreated($id, $email));

        return $self;
    }

    #[Apply(ProfileCreated::class)]
    protected function applyProfileCreated1(ProfileCreated $event): void
    {
        $this->id = $event->profileId;
        $this->email = $event->email;
    }

    #[Apply(ProfileCreated::class)]
    protected function applyProfileCreated2(ProfileCreated $event): void
    {
        $this->id = $event->profileId;
        $this->email = $event->email;
    }
}
