<?php

namespace Patchlevel\EventSourcing\EventBus;

interface AfterDispatchMiddleware
{
    public function afterDispatch(Message ...$messages): void;
}