<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Subscription\Subscriber;

use Patchlevel\EventSourcing\Attribute\Subscriber;
use Patchlevel\EventSourcing\Metadata\Subscriber\AttributeSubscriberMetadataFactory;
use Patchlevel\EventSourcing\Subscription\RunMode;
use Patchlevel\EventSourcing\Subscription\Subscriber\ArgumentResolver;
use Patchlevel\EventSourcing\Subscription\Subscriber\MetadataSubscriberAccessor;
use Patchlevel\EventSourcing\Subscription\Subscriber\MetadataSubscriberAccessorRepository;
use PHPUnit\Framework\TestCase;

/** @covers \Patchlevel\EventSourcing\Subscription\Subscriber\MetadataSubscriberAccessorRepository */
final class MetadataSubscriberAccessorRepositoryTest extends TestCase
{
    public function testEmpty(): void
    {
        $repository = new MetadataSubscriberAccessorRepository(
            [],
        );

        self::assertEquals([], $repository->all());
        self::assertNull($repository->get('foo'));
    }

    public function testWithSubscriber(): void
    {
        $subscriber = new #[Subscriber('foo', RunMode::FromBeginning)]
        class {
        };
        $metadataFactory = new AttributeSubscriberMetadataFactory();

        $repository = new MetadataSubscriberAccessorRepository(
            [$subscriber],
            $metadataFactory,
        );

        $accessor = new MetadataSubscriberAccessor(
            $subscriber,
            $metadataFactory->metadata($subscriber::class),
            [
                new ArgumentResolver\MessageArgumentResolver(),
                new ArgumentResolver\EventArgumentResolver(),
                new ArgumentResolver\AggregateIdArgumentResolver(),
                new ArgumentResolver\RecordedOnArgumentResolver(),
            ],
        );

        self::assertEquals([$accessor], $repository->all());
        self::assertEquals($accessor, $repository->get('foo'));
    }
}
