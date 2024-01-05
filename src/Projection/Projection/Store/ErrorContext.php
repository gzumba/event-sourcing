<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Projection\Projection\Store;

use Throwable;

/** @psalm-type Context = array{message: string, code: int|string, file: string, line: int, trace: string} */
final class ErrorContext
{
    /** @return list<Context> */
    public static function fromThrowable(Throwable $error): array
    {
        $errors = [];

        do {
            $errors[] = self::transform($error);
            $error = $error->getPrevious();
        } while ($error);

        return $errors;
    }

    /** @return Context */
    private static function transform(Throwable $error): array
    {
        return [
            'message' => $error->getMessage(),
            'code' => $error->getCode(),
            'file' => $error->getFile(),
            'line' => $error->getLine(),
            'trace' => $error->getTraceAsString(),
        ];
    }
}
