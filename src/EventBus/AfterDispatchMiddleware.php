<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\EventBus;

interface AfterDispatchMiddleware
{
    public function afterDispatch(Message ...$messages): void;
}
