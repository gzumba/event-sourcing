<?php

namespace Patchlevel\EventSourcing\EventBus;

interface BeforeDispatchMiddleware
{
    public function beforeDispatch(Message ...$messages): void;
}