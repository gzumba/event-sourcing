<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Pipeline\Middleware;

use Patchlevel\EventSourcing\Pipeline\EventBucket;
use Patchlevel\EventSourcing\Pipeline\Middleware\RecalculatePlayheadMiddleware;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\Email;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\Profile;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileCreated;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileId;
use PHPUnit\Framework\TestCase;

/** @covers \Patchlevel\EventSourcing\Pipeline\Middleware\RecalculatePlayheadMiddleware */
class RecalculatePlayheadMiddlewareTest extends TestCase
{
    public function testReculatePlayhead(): void
    {
        $middleware = new RecalculatePlayheadMiddleware();

        $bucket = new EventBucket(
            Profile::class,
            1,
            ProfileCreated::raise(
                ProfileId::fromString('1'),
                Email::fromString('hallo@patchlevel.de')
            )->recordNow(5)
        );

        $result = $middleware($bucket);

        self::assertCount(1, $result);
        self::assertSame(Profile::class, $result[0]->aggregateClass());

        $event = $result[0]->event();

        self::assertSame(1, $event->playhead());
    }

    public function testReculatePlayheadWithSamePlayhead(): void
    {
        $middleware = new RecalculatePlayheadMiddleware();

        $bucket = new EventBucket(
            Profile::class,
            1,
            ProfileCreated::raise(
                ProfileId::fromString('1'),
                Email::fromString('hallo@patchlevel.de')
            )->recordNow(0)
        );

        $result = $middleware($bucket);

        self::assertCount(1, $result);
        self::assertSame(Profile::class, $result[0]->aggregateClass());

        $event = $result[0]->event();

        self::assertSame(1, $event->playhead());
    }
}
