<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Metadata\AggregateRoot;

use Patchlevel\EventSourcing\Aggregate\AggregateRootInterface;
use Patchlevel\EventSourcing\Metadata\MetadataException;

use function sprintf;

final class DuplicateApplyMethod extends MetadataException
{
    /**
     * @param class-string<AggregateRootInterface> $aggregate
     * @param class-string                         $event
     */
    public function __construct(string $aggregate, string $event, string $fistMethod, string $secondMethod)
    {
        parent::__construct(
            sprintf(
                'Two methods "%s" and "%s" on the aggregate "%s" want to apply the same event "%s". Only one method can apply an event.',
                $fistMethod,
                $secondMethod,
                $aggregate,
                $event
            )
        );
    }
}
