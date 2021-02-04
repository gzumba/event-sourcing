<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Console;

use InvalidArgumentException;
use Patchlevel\EventSourcing\Aggregate\AggregateRoot;
use Patchlevel\EventSourcing\Store\Store;
use Patchlevel\EventSourcing\Tool\Console\EventPrinter;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function array_flip;
use function is_string;

class ShowCommand extends Command
{
    private Store $store;

    /** @var array<class-string<AggregateRoot>, string> */
    private array $aggregates;

    /**
     * @param array<class-string<AggregateRoot>, string> $aggregates
     */
    public function __construct(Store $store, array $aggregates)
    {
        parent::__construct();

        $this->store = $store;
        $this->aggregates = $aggregates;
    }

    protected function configure(): void
    {
        $this
            ->setName('event-sourcing:show')
            ->setDescription('create projection schema')
            ->addArgument('aggregate', InputArgument::REQUIRED)
            ->addArgument('id', InputArgument::REQUIRED);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $console = new SymfonyStyle($input, $output);
        $dumper = new EventPrinter();

        $map = array_flip($this->aggregates);

        $aggregate = $input->getArgument('aggregate');
        $id = $input->getArgument('id');

        if (!is_string($aggregate) || !is_string($id)) {
            throw new InvalidArgumentException();
        }

        if (!array_key_exists($aggregate, $map)) {
            $console->error(sprintf('aggregate type "%s" not exists', $aggregate));

            return 1;
        }

        $events = $this->store->load($map[$aggregate], $id);

        if (count($events) === 0) {
            $console->error(sprintf('aggregate "%s" => "%s" not found', $aggregate, $id));

            return 1;
        }

        foreach ($events as $event) {
            $dumper->write($console, $event);
        }

        return 0;
    }
}
