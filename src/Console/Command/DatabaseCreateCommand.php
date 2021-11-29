<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Console\Command;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use InvalidArgumentException;
use Patchlevel\EventSourcing\Console\InputHelper;
use Patchlevel\EventSourcing\Store\DoctrineStore;
use Patchlevel\EventSourcing\Store\Store;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Throwable;

use function in_array;
use function is_string;
use function sprintf;

class DatabaseCreateCommand extends Command
{
    private Store $store;

    public function __construct(Store $store)
    {
        parent::__construct();

        $this->store = $store;
    }

    protected function configure(): void
    {
        $this
            ->setName('event-sourcing:database:create')
            ->setDescription('create eventstore database')
            ->addOption('if-not-exists', null, InputOption::VALUE_NONE, 'Don\'t trigger an error, when the database already exists');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $console = new SymfonyStyle($input, $output);
        $store = $this->store;

        if (!$store instanceof DoctrineStore) {
            $console->error('Store is not supported!');

            return 1;
        }

        $connection = $store->connection();

        $databaseName = $this->databaseName($connection);
        $tempConnection = $this->copyConnectionWithoutDatabase($connection);

        $ifNotExists = InputHelper::bool($input->getOption('if-not-exists'));
        $hasDatabase = in_array($databaseName, $tempConnection->createSchemaManager()->listDatabases());

        if ($ifNotExists && $hasDatabase) {
            $console->info(sprintf('Database "%s" already exists. Skipped.', $databaseName));
            $tempConnection->close();

            return 0;
        }

        try {
            $tempConnection->createSchemaManager()->createDatabase($databaseName);
            $console->info(sprintf('Created database "%s"', $databaseName));
        } catch (Throwable $e) {
            $console->error(sprintf('Could not create database "%s"', $databaseName));
            $console->error($e->getMessage());

            $tempConnection->close();

            return 2;
        }

        $tempConnection->close();

        return 0;
    }

    private function databaseName(Connection $connection): string
    {
        $params = $connection->getParams();

        if (isset($params['path']) && is_string($params['path'])) {
            return $params['path'];
        }

        if (isset($params['dbname']) && is_string($params['dbname'])) {
            return $params['dbname'];
        }

        throw new InvalidArgumentException(
            "Connection does not contain a 'path' or 'dbname' parameter and cannot be created."
        );
    }

    private function copyConnectionWithoutDatabase(Connection $connection): Connection
    {
        $params = $connection->getParams();

        unset($params['dbname'], $params['path'], $params['url']);

        $tmpConnection = DriverManager::getConnection($params);
        $tmpConnection->connect();

        return $tmpConnection;
    }
}
