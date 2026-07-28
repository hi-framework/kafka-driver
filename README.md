# hi/kafka-driver

The authoritative system design is [`../ARCHITECTURE.md`](../ARCHITECTURE.md). This file is only a source-review and test entry point.

## Source map

- `Client`, `SwooleClient`, `SwowClient`: public runtime entry points.
- `CoroutineConsumerStream`: runtime-neutral ordering of consumer IO, barriers and recovery.
- `ConsumerTransportInterface`: the narrow Swoole/Swow socket boundary.
- `Internal/ConsumerStreamState`: pure subscription identity, credit and settlement invariants.
- `Internal/ConsumerFrameReader`: connection-local incremental frame buffering.
- `Internal/ConsumerProtocolInterface`: replaceable boundary around extension codecs.

The `Internal` classes are public only because Composer must autoload them. They are not package API and may change without compatibility guarantees.

## Tests

```bash
composer test
```

The unit suite uses fake protocol, worker and transport implementations. It must run without the hi-kafka, Swoole or Swow extensions and without a Kafka broker. Real runtime coverage remains in `../tests/php`.
