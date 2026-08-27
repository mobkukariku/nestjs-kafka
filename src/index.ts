export * from './kafka.module';
export * from './kafka.module-definition';
export * from './kafka-producer.service';
export * from './kafka-consumer.service';
export * from './events/base.event';
export * from './types/kafka-module-options.interface';
export * from './types/kafka-event-handler.type';
export * from './kafka-client.factory';
export * from './kafka-health.indicator';
export * from './dedupe/dedupe.store';
export * from './dedupe/in-memory-dedupe.store';
// RedisDedupeStore намеренно не реэкспортится: он тянет ioredis, который нужен
// не всем. Импорт: `@mobkukariku/nestjs-kafka/redis`.
