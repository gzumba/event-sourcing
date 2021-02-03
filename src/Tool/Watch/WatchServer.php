<?php

declare(strict_types=1);

namespace Patchlevel\EventSourcing\Tool\Watch;

use DateTimeImmutable;
use Patchlevel\EventSourcing\Aggregate\AggregateChanged;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

use function base64_decode;
use function fclose;
use function feof;
use function fgets;
use function sprintf;
use function stream_select;
use function stream_socket_accept;
use function stream_socket_server;
use function strpos;
use function unserialize;

class WatchServer
{
    private string $host;

    /** @var resource|null */
    private $socket;

    private LoggerInterface $logger;

    public function __construct(string $host, ?LoggerInterface $logger = null)
    {
        if (strpos($host, '://') === false) {
            $host = 'tcp://' . $host;
        }

        $this->host = $host;
        $this->logger = $logger ?: new NullLogger();
    }

    public function start(): void
    {
        if (!$this->socket = stream_socket_server($this->host, $errno, $errstr)) {
            throw new RuntimeException(sprintf('Server start failed on "%s": ', $this->host) . $errstr . ' ' . $errno);
        }
    }

    /**
     * @param callable(AggregateChanged $event, int $clientId):void $callback
     */
    public function listen(callable $callback): void
    {
        if ($this->socket === null) {
            $this->start();
        }

        foreach ($this->getMessages() as $clientId => $message) {
            $this->logger->info('Received a payload from client {clientId}', ['clientId' => $clientId]);

            $payload = @unserialize(base64_decode($message), ['allowed_classes' => [DateTimeImmutable::class]]);
            $event = AggregateChanged::deserialize($payload);

            $callback($event, $clientId);
        }
    }

    public function getHost(): string
    {
        return $this->host;
    }

    private function getMessages(): iterable
    {
        $sockets = [(int)$this->socket => $this->socket];
        $write = [];

        while (true) {
            $read = $sockets;
            stream_select($read, $write, $write, null);

            foreach ($read as $stream) {
                if ($this->socket === $stream) {
                    $stream = stream_socket_accept($this->socket);
                    $sockets[(int)$stream] = $stream;
                } elseif (feof($stream)) {
                    unset($sockets[(int)$stream]);
                    fclose($stream);
                } else {
                    yield (int)$stream => fgets($stream);
                }
            }
        }
    }
}
