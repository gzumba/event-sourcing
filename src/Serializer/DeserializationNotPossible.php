<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Serializer;

use Patchlevel\EventSourcing\Aggregate\AggregateChanged;
use Throwable;

use function sprintf;

final class DeserializationNotPossible extends SerializeException
{
    /** @var class-string<AggregateChanged> */
    private string $eventClass;
    private string $data;

    /**
     * @param class-string<AggregateChanged> $eventClass
     */
    public function __construct(string $eventClass, string $data, ?Throwable $previous = null)
    {
        $this->eventClass = $eventClass;
        $this->data = $data;

        parent::__construct(
            sprintf(
                'deserialization of "%s" with "%s" data is not possible',
                $eventClass,
                $data
            ),
            0,
            $previous
        );
    }

    /**
     * @return class-string<AggregateChanged>
     */
    public function eventClass(): string
    {
        return $this->eventClass;
    }

    public function data(): string
    {
        return $this->data;
    }
}
