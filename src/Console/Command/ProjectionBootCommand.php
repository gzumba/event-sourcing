<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Console\Command;

use Patchlevel\EventSourcing\Console\InputHelper;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(
    'event-sourcing:projection:boot',
    'Prepare new projections and catch up with the event store',
)]
final class ProjectionBootCommand extends ProjectionCommand
{
    public function configure(): void
    {
        parent::configure();

        $this
            ->addOption(
                'limit',
                null,
                InputOption::VALUE_REQUIRED,
                'How many messages should be consumed in one run',
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $limit = InputHelper::nullablePositiveInt($input->getOption('limit'));

        $criteria = $this->projectionCriteria($input);
        $this->projectionist->boot($criteria, $limit);

        return 0;
    }
}