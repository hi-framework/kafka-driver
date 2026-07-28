<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

use Hi\Kafka\ConsumerTransportInterface;

/**
 * @internal 与协程运行时无关的增量式帧读取器，支持从合并读取结果中拆分帧
 */
final class ConsumerFrameReader
{
    private const READ_CHUNK_BYTES = 65536;
    private string $buffer = '';

    /**
     * 绑定独占 stream 的传输层与唯一协议编解码边界。
     */
    public function __construct(
        private readonly ConsumerTransportInterface $transport,
        private readonly ConsumerProtocolInterface $protocol,
    ) {
    }

    /**
     * 在同一截止时间内读取一个完整帧，并保留合并读取中属于后续帧的字节。
     *
     * 中途读超时不会清空缓冲，因为这些字节仍属于当前连接世代。
     */
    public function read(int $timeoutMs): ConsumerWireFrame
    {
        $deadline = \microtime(true) + ($timeoutMs / 1000);
        $headerLength = $this->protocol->headerLength();
        $this->fill($headerLength, $deadline);
        $header = $this->protocol->parseHeader(\substr($this->buffer, 0, $headerLength));
        $frameLength = $headerLength + $header['payload_len'];
        $this->fill($frameLength, $deadline);

        $raw = \substr($this->buffer, 0, $frameLength);
        $this->buffer = (string) \substr($this->buffer, $frameLength);
        return new ConsumerWireFrame($header['kind'], $header['cid'], $raw);
    }

    /**
     * 丢弃连接本地缓冲；仅在断线或切换连接世代时调用。
     */
    public function reset(): void
    {
        $this->buffer = '';
    }

    /**
     * 按统一截止时间持续增量读取，直到缓冲达到所需字节数。
     */
    private function fill(int $requiredBytes, float $deadline): void
    {
        while (\strlen($this->buffer) < $requiredBytes) {
            $remainingMs = (int) \max(0, \ceil(($deadline - \microtime(true)) * 1000));
            $this->buffer .= $this->transport->receive(
                \max(self::READ_CHUNK_BYTES, $requiredBytes - \strlen($this->buffer)),
                $remainingMs,
            );
        }
    }
}
