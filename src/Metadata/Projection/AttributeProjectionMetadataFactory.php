<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Metadata\Projection;

use Patchlevel\EventSourcing\Attribute\Create;
use Patchlevel\EventSourcing\Attribute\Drop;
use Patchlevel\EventSourcing\Attribute\Handle;
use Patchlevel\EventSourcing\EventBus\Message;
use Patchlevel\EventSourcing\Projection\Projection;
use ReflectionClass;
use ReflectionMethod;
use ReflectionNamedType;

use function array_key_exists;

final class AttributeProjectionMetadataFactory implements ProjectionMetadataFactory
{
    /** @var array<class-string<Projection>, ProjectionMetadata> */
    private array $projectionMetadata = [];

    /**
     * @param class-string<Projection> $projection
     */
    public function metadata(string $projection): ProjectionMetadata
    {
        if (array_key_exists($projection, $this->projectionMetadata)) {
            return $this->projectionMetadata[$projection];
        }

        $reflector = new ReflectionClass($projection);
        $methods = $reflector->getMethods();

        $metadata = new ProjectionMetadata();

        foreach ($methods as $method) {
            $attributes = $method->getAttributes(Handle::class);

            foreach ($attributes as $attribute) {
                $instance = $attribute->newInstance();
                $eventClass = $instance->eventClass();

                if (array_key_exists($eventClass, $metadata->handleMethods)) {
                    throw new DuplicateHandleMethod(
                        $projection,
                        $eventClass,
                        $metadata->handleMethods[$eventClass]->methodName,
                        $method->getName()
                    );
                }

                $metadata->handleMethods[$eventClass] = new ProjectionHandleMetadata(
                    $method->getName(),
                    $this->passMessage($method)
                );
            }

            if ($method->getAttributes(Create::class)) {
                if ($metadata->createMethod) {
                    throw new DuplicateCreateMethod(
                        $projection,
                        $metadata->createMethod,
                        $method->getName()
                    );
                }

                $metadata->createMethod = $method->getName();
            }

            if (!$method->getAttributes(Drop::class)) {
                continue;
            }

            if ($metadata->dropMethod) {
                throw new DuplicateDropMethod(
                    $projection,
                    $metadata->dropMethod,
                    $method->getName()
                );
            }

            $metadata->dropMethod = $method->getName();
        }

        $this->projectionMetadata[$projection] = $metadata;

        return $metadata;
    }

    private function passMessage(ReflectionMethod $method): bool
    {
        $parameters = $method->getParameters();

        if ($parameters === []) {
            return false;
        }

        $firstParameter = $parameters[0];
        $type = $firstParameter->getType();

        if (!$type instanceof ReflectionNamedType) {
            return false;
        }

        return $type->getName() === Message::class;
    }
}