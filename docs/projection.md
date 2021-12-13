# Projections

With `projections` you can create your data optimized for reading.
projections can be adjusted, deleted or rebuilt at any time.
This is possible because the source of truth remains untouched 
and everything can always be reproduced from the events.

The target of a projection can be anything. 
Either a file, a relational database, a no-sql database like mongodb or an elasticsearch.

In this example we always create a new data set in a relational database when a profile is created:

```php
<?php

declare(strict_types=1);

namespace App\Projection;

use Doctrine\DBAL\Connection;
use Patchlevel\EventSourcing\Aggregate\AggregateChanged;
use Patchlevel\EventSourcing\Projection\Projection;

final class ProfileProjection implements Projection
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /** @return iterable<class-string<AggregateChanged>, string> */
    public function handledEvents(): iterable
    {
        yield ProfileCreated::class => 'handleProfileCreated';
    }

    public function create(): void
    {
        $this->connection->executeStatement('CREATE TABLE IF NOT EXISTS projection_profile (id VARCHAR PRIMARY KEY);');
    }

    public function drop(): void
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS projection_profile;');
    }

    public function handleProfileCreated(ProfileCreated $profileCreated): void
    {
        $this->connection->executeStatement(
            'INSERT INTO projection_profile (`id`) VALUES(:id);',
            [
                'id' => $profileCreated->profileId(),
                'name' => $profileCreated->name()
            ]
        );
    }
}
```

> :warning: You should not execute any actions with projections, 
> otherwise these will be executed again if you rebuild the projection!

Projections have a `create` and a `drop` method that is executed when the projection is created or deleted.
In some cases it may be that no schema has to be created for the projection, as the target does it automatically.

Furthermore you have to implement the method `handledEvents`, which returns a hash map. 
The event class is mapped in the hash map with the appropriate method.

As soon as the event has been dispatched, the appropriate methods are then executed. 
Several projections can also listen to the same event.

## Register projections

So that the projections are known and also executed, you have to add them to the `ProjectionRepository`.
Then add this to the event bus using the `ProjectionListener`.

```php
use Patchlevel\EventSourcing\EventBus\DefaultEventBus;
use Patchlevel\EventSourcing\Projection\ProjectionListener;

$profileProjection = new ProfileProjection($connection);
$messageProjection = new MessageProjection($connection);

$projectionRepository = new DefaultProjectionRepository([
    $profileProjection,
    $messageProjection
]);

$eventBus->addListener(new ProjectionListener($projectionRepository));
```

> :book: You can find out more about the event bus [here](./event_bus.md). 