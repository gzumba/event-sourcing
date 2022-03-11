<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Pipeline\Middleware;

use Patchlevel\EventSourcing\Aggregate\AggregateChanged;
use Patchlevel\EventSourcing\EventBus\Message;
use Patchlevel\EventSourcing\Pipeline\Middleware\FilterEventMiddleware;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\Email;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\Profile;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileCreated;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileId;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileVisited;
use PHPUnit\Framework\TestCase;

/** @covers \Patchlevel\EventSourcing\Pipeline\Middleware\FilterEventMiddleware */
class FilterEventMiddlewareTest extends TestCase
{
    public function testPositive(): void
    {
        $middleware = new FilterEventMiddleware(static function (AggregateChanged $aggregateChanged) {
            return $aggregateChanged instanceof ProfileCreated;
        });

        $message = new Message(
            Profile::class,
            '1',
            1,
            ProfileCreated::raise(
                ProfileId::fromString('1'),
                Email::fromString('hallo@patchlevel.de')
            )
        );

        $result = $middleware($message);

        self::assertSame([$message], $result);
    }

    public function testNegative(): void
    {
        $middleware = new FilterEventMiddleware(static function (AggregateChanged $aggregateChanged) {
            return $aggregateChanged instanceof ProfileCreated;
        });

        $message = new Message(
            Profile::class,
            '1',
            1,
            ProfileVisited::raise(
                ProfileId::fromString('1'),
                ProfileId::fromString('2')
            )
        );

        $result = $middleware($message);

        self::assertSame([], $result);
    }
}
