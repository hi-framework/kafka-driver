<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * @internal 扩展协议编解码函数的无状态薄适配层
 */
final class ExtensionConsumerProtocol implements ConsumerProtocolInterface
{
    /**
     * 从扩展读取固定 envelope header 长度。
     */
    public function headerLength(): int
    {
        return \hi_kafka_header_len();
    }

    /**
     * 从扩展读取 Error frame 类型编号。
     */
    public function errorFrameKind(): int
    {
        return \hi_kafka_error_frame_kind();
    }

    /**
     * 通过扩展编码 protocol-v2 HELLO 帧。
     */
    public function helloFrame(): string
    {
        return \hi_kafka_encode_hello_frame(1);
    }

    /**
     * 通过扩展校验 HELLO 响应和协议版本。
     */
    public function verifyHello(string $frame): void
    {
        \hi_kafka_verify_hello_resp($frame);
    }

    /**
     * 通过扩展解析固定长度 frame header。
     */
    public function parseHeader(string $header): array
    {
        return \hi_kafka_parse_header($header);
    }

    /**
     * 通过扩展解码 Consumer stream 帧。
     */
    public function decodeStreamFrame(string $frame): array
    {
        return \hi_kafka_decode_stream_frame($frame);
    }

    /**
     * 通过扩展解码 Error 帧及错误元数据。
     */
    public function decodeErrorFrame(string $frame): array
    {
        return \hi_kafka_decode_error_frame($frame);
    }

    /**
     * 编码 ConsumeOpen，并把扩展生成的 cid 与 frame 封装为请求对象。
     */
    public function open(string $cluster, string $groupId, array $topics, array $config, int $maxMessages, int $maxBytes): ConsumerRequest
    {
        $request = \hi_kafka_encode_consume_open_frame($cluster, $groupId, $topics, $config, 0, 0, $maxMessages, $maxBytes);
        return new ConsumerRequest($request['cid'], $request['frame']);
    }

    /**
     * 编码 ConsumeResume，并把扩展生成的 cid 与 frame 封装为请求对象。
     */
    public function resume(int $subscriptionId, int $epoch, string $token): ConsumerRequest
    {
        $request = \hi_kafka_encode_consume_resume_frame($subscriptionId, $epoch, $token, 0, 0);
        return new ConsumerRequest($request['cid'], $request['frame']);
    }

    /**
     * 编码带可选原子信用额度补充的 ConsumeAck 帧。
     */
    public function ack(int $subscriptionId, int $epoch, int $deliveryId, int $messageCredit, int $byteCredit): string
    {
        return \hi_kafka_encode_consume_ack_frame($subscriptionId, $epoch, $deliveryId, $messageCredit, $byteCredit);
    }

    /**
     * 编码携带可选失败原因的 ConsumeNack 帧。
     */
    public function nack(int $subscriptionId, int $epoch, int $deliveryId, ?string $reason): string
    {
        return \hi_kafka_encode_consume_nack_frame($subscriptionId, $epoch, $deliveryId, $reason);
    }

    /**
     * 编码独立补充背压额度的 FlowControl 帧。
     */
    public function flow(int $subscriptionId, int $epoch, int $messageCredit, int $byteCredit): string
    {
        return \hi_kafka_encode_flow_frame($subscriptionId, $epoch, $messageCredit, $byteCredit);
    }

    /**
     * 编码 Commit，并把扩展生成的 cid 与 frame 封装为请求对象。
     */
    public function commit(int $subscriptionId, int $epoch): ConsumerRequest
    {
        $request = \hi_kafka_encode_consume_commit_frame($subscriptionId, $epoch);
        return new ConsumerRequest($request['cid'], $request['frame']);
    }

    /**
     * 编码正常关闭 subscription 的 ConsumeClose 帧。
     */
    public function close(int $subscriptionId, int $epoch): string
    {
        return \hi_kafka_encode_consume_close_frame($subscriptionId, $epoch);
    }

    /**
     * 编码暂停指定 assignment partitions 的控制帧。
     */
    public function pause(int $subscriptionId, int $epoch, array $partitions): string
    {
        return \hi_kafka_encode_consume_pause_frame($subscriptionId, $epoch, $partitions);
    }

    /**
     * 编码恢复指定 assignment partitions 的控制帧。
     */
    public function resumePartitions(int $subscriptionId, int $epoch, array $partitions): string
    {
        return \hi_kafka_encode_consume_partition_resume_frame($subscriptionId, $epoch, $partitions);
    }
}
