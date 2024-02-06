<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Serializer\Encoder;

use Patchlevel\EventSourcing\Serializer\Encoder\DecodeNotPossible;
use Patchlevel\EventSourcing\Serializer\Encoder\EncodeNotPossible;
use Patchlevel\EventSourcing\Serializer\Encoder\JsonEncoder;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\Email;
use Patchlevel\EventSourcing\Tests\Unit\Fixture\ProfileId;
use PHPUnit\Framework\TestCase;

/** @covers \Patchlevel\EventSourcing\Serializer\Encoder\JsonEncoder */
final class JsonEncoderTest extends TestCase
{
    private JsonEncoder $encoder;

    public function setUp(): void
    {
        $this->encoder = new JsonEncoder();
    }

    public function testEncode(): void
    {
        $data = [
            'profileId' => '1',
            'email' => 'info@patchlevel.de',
        ];

        self::assertEquals(
            '{"profileId":"1","email":"info@patchlevel.de"}',
            $this->encoder->encode($data),
        );
    }

    public function testEncodePrettify(): void
    {
        $data = [
            'profileId' => '1',
            'email' => 'info@patchlevel.de',
        ];
        self::assertEquals(
            '{
    "profileId": "1",
    "email": "info@patchlevel.de"
}',
            $this->encoder->encode($data, [JsonEncoder::OPTION_PRETTY_PRINT => true]),
        );
    }

    public function testEncodeError(): void
    {
        $data = [
            'foo' => ["\xF4\xBF\xBF\xBF̆" => 1],
        ];

        $this->expectException(EncodeNotPossible::class);
        $this->encoder->encode($data);
    }

    public function testEncodeNotNormalizedData(): void
    {
        $data = [
            'profileId' => ProfileId::fromString('1'),
            'email' => Email::fromString('info@patchlevel.de'),
        ];

        self::assertEquals(
            '{"profileId":{},"email":{}}',
            $this->encoder->encode($data),
        );
    }

    public function testDecode(): void
    {
        $expected = [
            'profileId' => '1',
            'email' => 'info@patchlevel.de',
        ];

        $event = $this->encoder->decode('{"profileId":"1","email":"info@patchlevel.de"}');

        self::assertEquals($expected, $event);
    }

    public function testDecodeWithSyntaxError(): void
    {
        $this->expectException(DecodeNotPossible::class);

        $this->encoder->decode('');
    }
}
