<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\EventBus\Serializer;

use DateTimeImmutable;
use Patchlevel\EventSourcing\EventBus\Message;
use Patchlevel\EventSourcing\EventBus\Serializer\DeserializeFailed;
use Patchlevel\EventSourcing\EventBus\Serializer\PhpNativeMessageSerializer;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileId;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileVisited;
use PHPUnit\Framework\TestCase;
use Prophecy\PhpUnit\ProphecyTrait;

/** @covers \Patchlevel\EventSourcing\EventBus\Serializer\PhpNativeMessageSerializer */
final class PhpNativeMessageSerializerTest extends TestCase
{
    use ProphecyTrait;

    public function testSerialize(): void
    {
        $event = new ProfileVisited(
            ProfileId::fromString('foo'),
        );

        $message = Message::create($event)
            ->withRecordedOn(new DateTimeImmutable('2020-01-01T20:00:00.000000+0100'));

        $nativeSerializer = new PhpNativeMessageSerializer();

        $content = $nativeSerializer->serialize($message);

        self::assertEquals('Tzo0MToiUGF0Y2hsZXZlbFxFdmVudFNvdXJjaW5nXEV2ZW50QnVzXE1lc3NhZ2UiOjI6e3M6NTA6IgBQYXRjaGxldmVsXEV2ZW50U291cmNpbmdcRXZlbnRCdXNcTWVzc2FnZQBoZWFkZXJzIjthOjE6e3M6MTA6InJlY29yZGVkT24iO086MTc6IkRhdGVUaW1lSW1tdXRhYmxlIjozOntzOjQ6ImRhdGUiO3M6MjY6IjIwMjAtMDEtMDEgMjA6MDA6MDAuMDAwMDAwIjtzOjEzOiJ0aW1lem9uZV90eXBlIjtpOjE7czo4OiJ0aW1lem9uZSI7czo2OiIrMDE6MDAiO319czo0ODoiAFBhdGNobGV2ZWxcRXZlbnRTb3VyY2luZ1xFdmVudEJ1c1xNZXNzYWdlAGV2ZW50IjtPOjU4OiJQYXRjaGxldmVsXEV2ZW50U291cmNpbmdcVGVzdHNcVW5pdFxGaXh0dXJlXFByb2ZpbGVWaXNpdGVkIjoxOntzOjk6InZpc2l0b3JJZCI7Tzo1MzoiUGF0Y2hsZXZlbFxFdmVudFNvdXJjaW5nXFRlc3RzXFVuaXRcRml4dHVyZVxQcm9maWxlSWQiOjE6e3M6NTc6IgBQYXRjaGxldmVsXEV2ZW50U291cmNpbmdcVGVzdHNcVW5pdFxGaXh0dXJlXFByb2ZpbGVJZABpZCI7czozOiJmb28iO319fQ==', $content);
    }

    public function testDeserialize(): void
    {
        $event = new ProfileVisited(ProfileId::fromString('foo'));
        $nativeSerializer = new PhpNativeMessageSerializer();

        $message = $nativeSerializer->deserialize('Tzo0MToiUGF0Y2hsZXZlbFxFdmVudFNvdXJjaW5nXEV2ZW50QnVzXE1lc3NhZ2UiOjI6e3M6NTA6IgBQYXRjaGxldmVsXEV2ZW50U291cmNpbmdcRXZlbnRCdXNcTWVzc2FnZQBoZWFkZXJzIjthOjE6e3M6MTA6InJlY29yZGVkT24iO086MTc6IkRhdGVUaW1lSW1tdXRhYmxlIjozOntzOjQ6ImRhdGUiO3M6MjY6IjIwMjAtMDEtMDEgMjA6MDA6MDAuMDAwMDAwIjtzOjEzOiJ0aW1lem9uZV90eXBlIjtpOjE7czo4OiJ0aW1lem9uZSI7czo2OiIrMDE6MDAiO319czo0ODoiAFBhdGNobGV2ZWxcRXZlbnRTb3VyY2luZ1xFdmVudEJ1c1xNZXNzYWdlAGV2ZW50IjtPOjU4OiJQYXRjaGxldmVsXEV2ZW50U291cmNpbmdcVGVzdHNcVW5pdFxGaXh0dXJlXFByb2ZpbGVWaXNpdGVkIjoxOntzOjk6InZpc2l0b3JJZCI7Tzo1MzoiUGF0Y2hsZXZlbFxFdmVudFNvdXJjaW5nXFRlc3RzXFVuaXRcRml4dHVyZVxQcm9maWxlSWQiOjE6e3M6NTc6IgBQYXRjaGxldmVsXEV2ZW50U291cmNpbmdcVGVzdHNcVW5pdFxGaXh0dXJlXFByb2ZpbGVJZABpZCI7czozOiJmb28iO319fQ==');

        self::assertEquals($event, $message->event());
        self::assertEquals(
            [
                'recordedOn' => new DateTimeImmutable('2020-01-01T20:00:00.000000+0100'),
            ],
            $message->headers(),
        );
    }

    public function testDeserializeDecodeFailed(): void
    {
        $this->expectException(DeserializeFailed::class);

        $nativeSerializer = new PhpNativeMessageSerializer();

        $nativeSerializer->deserialize('!@#%$^&*()');
    }

    public function testDeserializeNotAMessage(): void
    {
        $this->expectException(DeserializeFailed::class);

        $nativeSerializer = new PhpNativeMessageSerializer();

        $nativeSerializer->deserialize('Tzo4OiJzdGRDbGFzcyI6MDp7fQ==');
    }

    public function testEquals(): void
    {
        $event = new ProfileVisited(
            ProfileId::fromString('foo'),
        );

        $message = Message::create($event)
            ->withRecordedOn(new DateTimeImmutable('2020-01-01T20:00:00.000000+0100'));

        $nativeSerializer = new PhpNativeMessageSerializer();

        $content = $nativeSerializer->serialize($message);
        $clonedMessage = $nativeSerializer->deserialize($content);

        self::assertEquals($message, $clonedMessage);
    }
}