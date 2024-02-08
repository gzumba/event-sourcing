<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Outbox;

use Patchlevel\EventSourcing\EventBus\AfterDispatchMiddleware;
use Patchlevel\EventSourcing\EventBus\Message;

final class OutboxEventBusMiddleware implements AfterDispatchMiddleware
{
    public function __construct(
        private readonly OutboxStore $store,
    ) {
    }

    public function afterDispatch(Message ...$messages): void
    {
        $this->store->saveOutboxMessage(...$messages);
    }
}
