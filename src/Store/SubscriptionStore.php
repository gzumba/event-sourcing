<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Store;

interface SubscriptionStore
{
    public function setupSubscription(): void;

    public function wait(): void;
}
