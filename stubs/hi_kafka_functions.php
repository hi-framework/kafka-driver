<?php

declare(strict_types=1);

// Protocol-v2 extension function declarations for static analysis.

if (! \function_exists('hi_kafka_version')) {
    function hi_kafka_version(): string
    {
        return '';
    }
    function hi_kafka_ensure_worker(?string $socket = null): void
    {
    }
    /**
     * @param array<string,string> $config
     */
    function hi_kafka_register_cluster(string $cluster, array $config, ?string $socket = null, ?int $timeoutMs = null): void
    {
    }
    function hi_kafka_header_len(): int
    {
        return 13;
    }
    function hi_kafka_error_frame_kind(): int
    {
        return 0x40;
    }
    function hi_kafka_next_cid(): int
    {
        return 0;
    }
    function hi_kafka_encode_hello_frame(?int $role = 0): string
    {
        return '';
    }
    function hi_kafka_verify_hello_resp(string $bytes): void
    {
    }
    /**
     * @return array{kind:int,cid:int,payload_len:int}
     */
    function hi_kafka_parse_header(string $bytes): array
    {
        return [];
    }
    /**
     * @return array{kind:int,kind_name:string,retryable:bool,outcome:string,native_code:int,message:string}
     */
    function hi_kafka_decode_error_frame(string $bytes): array
    {
        return [];
    }
    /**
     * @return array{cid:int,frame:string}
     */
    function hi_kafka_encode_register_cluster_frame(string $cluster, array $config): array
    {
        return [];
    }
    /**
     * @return array<string,mixed>
     */
    function hi_kafka_decode_control_resp(string $bytes): array
    {
        return [];
    }
    /**
     * @return array{cid:int,frame:string}
     */
    function hi_kafka_encode_consume_open_frame(string $cluster, string $groupId, array $topics, ?array $config, int $initialMessageCredit, int $initialByteCredit, int $maxInFlightMessages, int $maxInFlightBytes): array
    {
        return [];
    }
    /**
     * @return array{cid:int,frame:string}
     */
    function hi_kafka_encode_consume_resume_frame(int $subscriptionId, int $previousEpoch, string $token, int $messageCredit, int $byteCredit): array
    {
        return [];
    }
    function hi_kafka_encode_consume_ack_frame(int $subscriptionId, int $epoch, int $deliveryId, ?int $messageCredit = 0, ?int $byteCredit = 0): string
    {
        return '';
    }
    function hi_kafka_encode_consume_nack_frame(int $subscriptionId, int $epoch, int $deliveryId, ?string $reason = null): string
    {
        return '';
    }
    function hi_kafka_encode_flow_frame(int $subscriptionId, int $epoch, int $messageCredit, int $byteCredit): string
    {
        return '';
    }
    /**
     * @return array{cid:int,frame:string}
     */
    function hi_kafka_encode_consume_commit_frame(int $subscriptionId, int $epoch): array
    {
        return [];
    }
    function hi_kafka_encode_consume_close_frame(int $subscriptionId, int $epoch): string
    {
        return '';
    }
    function hi_kafka_encode_consume_pause_frame(int $subscriptionId, int $epoch, array $partitions): string
    {
        return '';
    }
    function hi_kafka_encode_consume_partition_resume_frame(int $subscriptionId, int $epoch, array $partitions): string
    {
        return '';
    }
    /**
     * @return array<string,mixed>
     */
    function hi_kafka_decode_stream_frame(string $bytes): array
    {
        return [];
    }
    /**
     * @return array{cid:int,frame:string}
     */
    function hi_kafka_encode_set_oauth_token_frame(string $cluster, string $token, int $lifetimeMs, string $principalName, ?array $extensions = null): array
    {
        return [];
    }
}
