<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Projection\Projector;

use Patchlevel\EventSourcing\Metadata\Projector\AttributeProjectorMetadataFactory;
use Patchlevel\EventSourcing\Metadata\Projector\ProjectorMetadataFactory;

trait ProjectorUtil
{
    private static ProjectorMetadataFactory|null $metadataFactory = null;

    public static function setMetadataFactory(ProjectorMetadataFactory $metadataFactory): void
    {
        self::$metadataFactory = $metadataFactory;
    }

    private static function metadataFactory(): ProjectorMetadataFactory
    {
        if (self::$metadataFactory === null) {
            self::$metadataFactory = new AttributeProjectorMetadataFactory();
        }

        return self::$metadataFactory;
    }

    private function getProjectorHelper(): ProjectorHelper
    {
        return new ProjectorHelper(self::metadataFactory());
    }

    private function projectorId(): string
    {
        return $this->getProjectorHelper()->projectorId($this);
    }
}