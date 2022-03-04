<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Projection;

use Patchlevel\EventSourcing\Attribute\Handle;
use Patchlevel\EventSourcing\Projection\BaseProjection;
use Patchlevel\EventSourcing\Projection\DuplicateHandleMethod;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\MessagePublished;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileCreated;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileVisited;
use PHPUnit\Framework\TestCase;

/** @coversNothing  */
class AttributeHandleMethodTest extends TestCase
{
    public function testHandleAttribute(): void
    {
        $projection = new class extends BaseProjection {
            #[Handle(ProfileCreated::class)]
            #[Handle(ProfileVisited::class)]
            public function handleProfileCreated(ProfileCreated|ProfileVisited $event): void
            {
            }

            #[Handle(MessagePublished::class)]
            public function handlePublish(MessagePublished $event): void
            {
            }
        };

        self::assertSame(
            [
                ProfileCreated::class => 'handleProfileCreated',
                ProfileVisited::class => 'handleProfileCreated',
                MessagePublished::class => 'handlePublish',
            ],
            $projection->handledEvents()
        );
    }

    public function testDuplicateHandleAttribute(): void
    {
        $this->expectException(DuplicateHandleMethod::class);

        $projection = new class extends BaseProjection {
            #[Handle(ProfileCreated::class)]
            public function handleProfileCreated1(ProfileCreated $event): void
            {
            }

            #[Handle(ProfileCreated::class)]
            public function handleProfileCreated2(ProfileCreated $event): void
            {
            }

            public function create(): void
            {
            }

            public function drop(): void
            {
            }
        };

        $projection->handledEvents();
    }
}
