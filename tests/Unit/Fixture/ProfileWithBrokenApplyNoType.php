<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Fixture;

use Patchlevel\EventSourcing\Aggregate\BasicAggregateRoot;
use Patchlevel\EventSourcing\Attribute\Aggregate;
use Patchlevel\EventSourcing\Attribute\AggregateId;
use Patchlevel\EventSourcing\Attribute\Apply;

#[Aggregate(ProfileWithBrokenApplyNoType::class)]
final class ProfileWithBrokenApplyNoType extends BasicAggregateRoot
{
    #[AggregateId]
    private ProfileId $id;

    /** @phpcsSuppress SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingAnyTypeHint */
    #[Apply]
    protected function applyWithNoType($event): void
    {
    }
}
