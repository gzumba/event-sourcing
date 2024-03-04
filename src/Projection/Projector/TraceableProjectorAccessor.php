<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Projection\Projector;

use Closure;
use Patchlevel\EventSourcing\EventBus\Message;
use Patchlevel\EventSourcing\Projection\Projection\RunMode;
use Patchlevel\EventSourcing\Repository\MessageDecorator\Trace;
use Patchlevel\EventSourcing\Repository\MessageDecorator\TraceStack;

use function array_map;

/** @experimental */
final class TraceableProjectorAccessor implements ProjectorAccessor
{
    public function __construct(
        private readonly ProjectorAccessor $parent,
        private readonly TraceStack $traceStack,
    ) {
    }

    public function id(): string
    {
        return $this->parent->id();
    }

    public function group(): string
    {
        return $this->parent->group();
    }

    public function runMode(): RunMode
    {
        return $this->parent->runMode();
    }

    public function setupMethod(): Closure|null
    {
        return $this->parent->setupMethod();
    }

    public function teardownMethod(): Closure|null
    {
        return $this->parent->teardownMethod();
    }

    /**
     * @param class-string $eventClass
     *
     * @return list<Closure(Message):void>
     */
    public function subscribeMethods(string $eventClass): array
    {
        return array_map(
            /**
             * @param Closure(Message):void $closure
             *
             * @return Closure(Message):void
             */
            fn (Closure $closure) => function (Message $message) use ($closure): void {
                $trace = new Trace(
                    $this->id(),
                    'event_sourcing/projector/' . $this->group(),
                );

                $this->traceStack->add($trace);

                try {
                    $closure($message);
                } finally {
                    $this->traceStack->remove($trace);
                }
            },
            $this->parent->subscribeMethods($eventClass),
        );
    }
}
