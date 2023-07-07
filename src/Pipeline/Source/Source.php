<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Pipeline\Source;

use Patchlevel\EventSourcing\EventBus\Message;

interface Source
{
    /** @return iterable<Message> */
    public function load(): iterable;

    public function count(): int;
}
