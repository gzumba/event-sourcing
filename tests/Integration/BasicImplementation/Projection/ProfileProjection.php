<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Integration\BasicImplementation\Projection;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;
use Patchlevel\EventSourcing\Attribute\Create;
use Patchlevel\EventSourcing\Attribute\Drop;
use Patchlevel\EventSourcing\Attribute\Projection;
use Patchlevel\EventSourcing\Attribute\Subscribe;
use Patchlevel\EventSourcing\EventBus\Message;
use Patchlevel\EventSourcing\Projection\Projector\BasicProjector;
use Patchlevel\EventSourcing\Tests\Integration\BasicImplementation\Events\ProfileCreated;

use function assert;

#[Projection('profile', 1)]
final class ProfileProjection extends BasicProjector
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    #[Create]
    public function create(): void
    {
        $table = new Table('projection_profile');
        $table->addColumn('id', 'string');
        $table->addColumn('name', 'string');
        $table->setPrimaryKey(['id']);

        $this->connection->createSchemaManager()->createTable($table);
    }

    #[Drop]
    public function drop(): void
    {
        $this->connection->createSchemaManager()->dropTable('projection_profile');
    }

    #[Subscribe(ProfileCreated::class)]
    public function handleProfileCreated(Message $message): void
    {
        $profileCreated = $message->event();

        assert($profileCreated instanceof ProfileCreated);

        $this->connection->executeStatement(
            'INSERT INTO projection_profile (id, name) VALUES(:id, :name);',
            [
                'id' => $profileCreated->profileId->toString(),
                'name' => $profileCreated->name,
            ],
        );
    }
}
