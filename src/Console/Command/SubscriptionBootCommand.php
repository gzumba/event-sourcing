<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Console\Command;

use Closure;
use Patchlevel\EventSourcing\Console\InputHelper;
use Patchlevel\EventSourcing\Store\Store;
use Patchlevel\EventSourcing\Store\SubscriptionStore;
use Patchlevel\EventSourcing\Subscription\Engine\SubscriptionEngine;
use Patchlevel\EventSourcing\Subscription\Engine\SubscriptionEngineCriteria;
use Patchlevel\Worker\DefaultWorker;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(
    'event-sourcing:subscription:boot',
    'Catch up with the event store.',
)]
final class SubscriptionBootCommand extends SubscriptionCommand
{
    public function __construct(
        SubscriptionEngine $engine,
        private readonly Store $store,
    ) {
        parent::__construct($engine);
    }

    public function configure(): void
    {
        parent::configure();

        $this
            ->addOption(
                'run-limit',
                null,
                InputOption::VALUE_REQUIRED,
                'The maximum number of runs this command should execute',
            )
            ->addOption(
                'message-limit',
                null,
                InputOption::VALUE_REQUIRED,
                'How many messages should be consumed in one run',
                1000,
            )
            ->addOption(
                'memory-limit',
                null,
                InputOption::VALUE_REQUIRED,
                'How much memory consumption should the worker be terminated (e.g. 250MB)',
            )
            ->addOption(
                'time-limit',
                null,
                InputOption::VALUE_REQUIRED,
                'What is the maximum time the worker can run in seconds',
            )
            ->addOption(
                'sleep',
                null,
                InputOption::VALUE_REQUIRED,
                'How much time should elapse before the next job is executed in milliseconds',
            )
            ->addOption(
                'setup',
                null,
                InputOption::VALUE_NONE,
                'Setup new subscriptions',
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $runLimit = InputHelper::nullablePositiveInt($input->getOption('run-limit'));
        $messageLimit = InputHelper::nullablePositiveInt($input->getOption('message-limit'));
        $memoryLimit = InputHelper::nullableString($input->getOption('memory-limit'));
        $timeLimit = InputHelper::nullablePositiveInt($input->getOption('time-limit'));
        $sleep = InputHelper::positiveIntOrZero($input->getOption('sleep'));
        $setup = InputHelper::bool($input->getOption('setup'));

        $criteria = $this->subscriptionEngineCriteria($input);
        $criteria = $this->resolveCriteriaIntoCriteriaWithOnlyIds($criteria);

        if ($this->store instanceof SubscriptionStore) {
            $this->store->setupSubscription();
        }

        if ($setup) {
            $this->engine->setup($criteria);
        }

        $logger = new ConsoleLogger($output);

        $finished = false;

        $worker = DefaultWorker::create(
            function (Closure $stop) use ($criteria, $messageLimit, &$finished, $sleep): void {
                $this->engine->boot($criteria, $messageLimit);

                if ($this->isBootingFinished($criteria)) {
                    $finished = true;
                    $stop();

                    return;
                }

                if (!$this->store instanceof SubscriptionStore) {
                    return;
                }

                $this->store->wait($sleep);
            },
            [
                'runLimit' => $runLimit,
                'memoryLimit' => $memoryLimit,
                'timeLimit' => $timeLimit,
            ],
            $logger,
        );

        $supportSubscription = $this->store instanceof SubscriptionStore && $this->store->supportSubscription();
        $worker->run($supportSubscription ? 0 : $sleep);

        return $finished ? 0 : 1;
    }

    private function isBootingFinished(SubscriptionEngineCriteria $criteria): bool
    {
        $subscriptions = $this->engine->subscriptions($criteria);

        foreach ($subscriptions as $subscription) {
            if ($subscription->isBooting()) {
                return false;
            }
        }

        return true;
    }
}