<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * @internal 由 ext-kafka 实现的二进制协议边界
 */
interface ConsumerProtocolInterface
{
    /**
     * 返回固定 frame envelope header 的字节长度。
     */
    public function headerLength(): int;

    /**
     * 返回协议 Error frame 的类型编号。
     */
    public function errorFrameKind(): int;

    /**
     * 编码连接建立后必须首先发送的 HELLO 帧。
     */
    public function helloFrame(): string;

    /**
     * 校验 worker 的 HELLO 响应及 protocol major 兼容性。
     */
    public function verifyHello(string $frame): void;

    /**
     * 解析固定长度 header，供 frame reader 确定完整帧边界。
     *
     * @return array{kind:int,cid:int,payload_len:int}
     */
    public function parseHeader(string $header): array;

    /**
     * 解码 Consumer 数据面或控制通知帧。
     *
     * @return array<string,mixed>
     */
    public function decodeStreamFrame(string $frame): array;

    /**
     * 解码 Error 帧为稳定的机器可读错误字段。
     *
     * @return array{kind:int,kind_name:string,retryable:bool,outcome:string,native_code:int,message:string}
     */
    public function decodeErrorFrame(string $frame): array;

    /**
     * 编码新建 subscription 的 ConsumeOpen 请求。
     *
     * @param list<string>         $topics
     * @param array<string,string> $config
     */
    public function open(string $cluster, string $groupId, array $topics, array $config, int $maxMessages, int $maxBytes): ConsumerRequest;

    /**
     * 编码使用 resume token 恢复既有 subscription 的请求。
     */
    public function resume(int $subscriptionId, int $epoch, string $token): ConsumerRequest;

    /**
     * 编码确认一条 delivery 并可原子补充信用额度的 ACK 帧。
     */
    public function ack(int $subscriptionId, int $epoch, int $deliveryId, int $messageCredit, int $byteCredit): string;

    /**
     * 编码拒绝一条 delivery 并携带可选失败原因的 NACK 帧。
     */
    public function nack(int $subscriptionId, int $epoch, int $deliveryId, ?string $reason): string;

    /**
     * 编码独立补充消息数与字节数额度的 FlowControl 帧。
     */
    public function flow(int $subscriptionId, int $epoch, int $messageCredit, int $byteCredit): string;

    /**
     * 编码提交连续 ACK 水位的 Commit 请求。
     */
    public function commit(int $subscriptionId, int $epoch): ConsumerRequest;

    /**
     * 编码正常关闭当前 subscription 的 ConsumeClose 帧。
     */
    public function close(int $subscriptionId, int $epoch): string;

    /**
     * 编码暂停当前 assignment 中指定 partitions 的控制帧。
     *
     * @param list<array{topic:string,partition:int}> $partitions
     */
    public function pause(int $subscriptionId, int $epoch, array $partitions): string;

    /**
     * 编码恢复当前 assignment 中指定 partitions 的控制帧。
     *
     * @param list<array{topic:string,partition:int}> $partitions
     */
    public function resumePartitions(int $subscriptionId, int $epoch, array $partitions): string;
}
