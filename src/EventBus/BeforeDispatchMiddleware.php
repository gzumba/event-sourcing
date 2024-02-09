<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\EventBus;

interface BeforeDispatchMiddleware
{
    public function beforeDispatch(Message ...$messages): void;
}
