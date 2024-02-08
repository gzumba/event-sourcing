<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\WatchServer;

use Patchlevel\EventSourcing\EventBus\BeforeDispatchMiddleware;
use Patchlevel\EventSourcing\EventBus\Message;

final class WatchEventBusMiddleware implements BeforeDispatchMiddleware
{
    public function __construct(
        private readonly WatchServerClient $watchServerClient,
    ) {
    }

    public function beforeDispatch(Message ...$messages): void
    {
        try {
            foreach ($messages as $message) {
                $this->watchServerClient->send($message);
            }
        } catch (SendingFailed) {
            // to nothing
        }
    }
}
