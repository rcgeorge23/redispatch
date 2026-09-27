# Redispatch

Redispatch is a small Java library for Redis Streams queues with at-least-once delivery, explicit acknowledgement, stale-message recovery, and bounded dead-letter streams.

## Requirements

- Java 25 or later
- Redis 6.2 or later (stale-message lookup uses the `XPENDING` idle-time filter)

## Install

Add Redispatch from Maven Central:

```groovy
dependencies {
    implementation 'io.github.rcgeorge23:redispatch:<version>'
}
```

The library exposes Spring Data Redis and Jackson types in its public constructors, so those dependencies are available transitively.
Provide a Redis connection driver in your application as well, such as Lettuce or a Spring Boot Redis starter; Redispatch uses the supplied `StringRedisTemplate` and does not create a connection factory.

## Use a Redis Streams queue

Create a queue with a `StringRedisTemplate`, an `ObjectMapper`, the payload's Jackson type, and queue options:

```java
RedisStreamQueue<OrderEvent> queue = new RedisStreamQueue<>(
        stringRedisTemplate,
        objectMapper,
        objectMapper.constructType(OrderEvent.class),
        RedisStreamQueueOptions.defaults("orders:events", "order-workers"));

String messageId = queue.publish(new OrderEvent("order-42"));
for (QueueMessage<OrderEvent> message : queue.pollBatch(25)) {
    persistInTransaction(message.payload());
    queue.acknowledge(message); // Acknowledge only after durable work commits.
}
```

Each queue uses a Redis Stream and a consumer group. A newly created group starts at the beginning of the stream. Configure a distinct consumer name for each worker instance when supplying explicit `RedisStreamQueueOptions`.

## Delivery and recovery

- Delivery is **at least once**. A process can repeat work if it fails after committing its side effects but before acknowledging the message; make durable handlers idempotent where possible.
- `acknowledge` acknowledges the group entry and removes it from the active stream. Call it only after the transaction or other durable side effect succeeds.
- `release` leaves an entry pending. It does not put the entry back into the ready stream immediately. `reclaimStale` makes it available to a worker after `reclaimMinIdle` has elapsed.
- Reclaim uses Redis's pending-entry idle-time filter, available since Redis 6.2. A reclaimed entry should be treated as a repeated delivery.
- `deadLetter` stores the payload, a non-blank failure code, and the original message ID in the dead-letter stream. The active entry is acknowledged only after the dead-letter write and trim succeed. `deadLetterMaxSize` bounds retained history; zero disables retention.
- Payloads that cannot be decoded are dead-lettered with the `deserialisation_failed` code and removed from the active queue only after that write succeeds.
- `backlogSize`, `deadLetterSize`, and `oldestPendingIdleMillis` expose basic queue diagnostics. The oldest-idle method returns `-1` when there are no pending entries.
- `purge` deletes active and dead-letter streams and recreates the consumer group. Use it only after coordinating all consumers and ensuring they are quiescent.

## In-memory adapter

`InMemoryMessageQueue<T>` implements the same queue interface for local code and tests without Redis. It preserves pending/reclaim timing semantics and accepts an injectable `Clock` for deterministic tests. It is not a distributed production queue.

## Tests

Run unit tests:

```sh
./gradlew test
```

Run the integration suite against a Redis server reachable at `REDIS_HOST` and `REDIS_PORT` (defaults to `localhost:6379`):

```sh
REDIS_HOST=localhost REDIS_PORT=6379 ./gradlew integrationTest
```

`./gradlew check` runs both the unit and Redis integration suites. The GitHub Actions workflow starts Redis 7.4 for these checks.

## Releases

Each push to `main` runs unit and Redis integration tests. If they pass, CI publishes a new version to Maven Central and creates the matching `v<version>` Git tag. Pull requests run verification only.

## License

Redispatch is available under the Apache License 2.0. See [LICENSE](LICENSE).
