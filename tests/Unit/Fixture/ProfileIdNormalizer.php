<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tests\Unit\Fixture;

use InvalidArgumentException;
use Patchlevel\EventSourcing\Serializer\Normalizer\InvalidArgument;
use Patchlevel\EventSourcing\Serializer\Normalizer\Normalizer;

use function is_string;

final class ProfileIdNormalizer implements Normalizer
{
    public function normalize(mixed $value): string
    {
        if (!$value instanceof ProfileId) {
            throw new InvalidArgumentException();
        }

        return $value->toString();
    }

    public function denormalize(mixed $value): ?ProfileId
    {
        if ($value === null) {
            return null;
        }

        if (!is_string($value)) {
            throw new InvalidArgument();
        }

        return ProfileId::fromString($value);
    }
}
