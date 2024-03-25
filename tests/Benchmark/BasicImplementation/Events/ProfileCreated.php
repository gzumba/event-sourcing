<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Benchmark\BasicImplementation\Events;

use Patchlevel\EventSourcing\Attribute\DataSubjectId;
use Patchlevel\EventSourcing\Attribute\Event;
use Patchlevel\EventSourcing\Attribute\PersonalData;
use Patchlevel\EventSourcing\Serializer\Normalizer\IdNormalizer;
use Patchlevel\EventSourcing\Tests\Benchmark\BasicImplementation\ProfileId;

#[Event('profile.created')]
final class ProfileCreated
{
    public function __construct(
        #[IdNormalizer]
        #[DataSubjectId]
        public ProfileId $profileId,
        public string $name,
        #[PersonalData]
        public string|null $email,
    ) {
    }
}
