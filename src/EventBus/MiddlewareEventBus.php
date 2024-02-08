<?php

namespace Patchlevel\EventSourcing\EventBus;

final class MiddlewareEventBus implements EventBus
{
    /**
     * @param EventBus $eventBus
     * @param iterable<BeforeDispatchMiddleware|AfterDispatchMiddleware> $middlewares
     */
    public function __construct(
        private readonly EventBus $eventBus,
        private readonly iterable $middlewares
    )
    {
    }

    public function dispatch(Message ...$messages): void
    {
        foreach ($this->middlewares as $middleware) {
            if ($middleware instanceof BeforeDispatchMiddleware) {
                $middleware->beforeDispatch(...$messages);
            }
        }

        $this->eventBus->dispatch(...$messages);

        foreach ($this->middlewares as $middleware) {
            if ($middleware instanceof AfterDispatchMiddleware) {
                $middleware->afterDispatch(...$messages);
            }
        }
    }
}