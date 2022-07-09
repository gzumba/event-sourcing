<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Fixture;

use Patchlevel\EventSourcing\Attribute\Event;
use Patchlevel\EventSourcing\Attribute\Normalize;

#[Event('wrong_normalizer')]
final class WrongNormalizerEvent
{
    public function __construct(
        #[Normalize(new EmailNormalizer())]
        public bool $email
    ) {
    }
}
