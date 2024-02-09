<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\EventBus;

final class MiddlewareEventBus implements EventBus
{
    /** @param iterable<BeforeDispatchMiddleware|AfterDispatchMiddleware> $middlewares */
    public function __construct(
        private readonly EventBus $eventBus,
        private readonly iterable $middlewares,
    ) {
    }

    public function dispatch(Message ...$messages): void
    {
        foreach ($this->middlewares as $middleware) {
            if (!($middleware instanceof BeforeDispatchMiddleware)) {
                continue;
            }

            $middleware->beforeDispatch(...$messages);
        }

        $this->eventBus->dispatch(...$messages);

        foreach ($this->middlewares as $middleware) {
            if (!($middleware instanceof AfterDispatchMiddleware)) {
                continue;
            }

            $middleware->afterDispatch(...$messages);
        }
    }
}
