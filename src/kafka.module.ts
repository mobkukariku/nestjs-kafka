// nestjs-kafka/src/kafka.module.ts
import { Module } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaConsumerService } from './kafka-consumer.service';
import { KafkaHealthIndicator } from './kafka-health.indicator';
import {
  ConfigurableModuleClass, KAFKA_CLIENT, MODULE_OPTIONS_TOKEN,
} from './kafka.module-definition';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';
import { createKafkaClient } from './kafka-client.factory';
import { DEDUPE_STORE } from './dedupe/dedupe.store';
import { InMemoryDedupeStore } from './dedupe/in-memory-dedupe.store';

// Глобальность управляется через forRoot({ isGlobal }) — см. kafka.module-definition.
@Module({
  providers: [
    {
      provide: KAFKA_CLIENT,
      useFactory: (options: KafkaModuleOptions) => createKafkaClient(options),
      inject: [MODULE_OPTIONS_TOKEN],
    },
    {
      provide: DEDUPE_STORE,
      useFactory: (options: KafkaModuleOptions) =>
        options.dedupe ?? new InMemoryDedupeStore(),
      inject: [MODULE_OPTIONS_TOKEN],
    },
    KafkaProducerService,
    KafkaConsumerService,
    KafkaHealthIndicator,
  ],
  exports: [KafkaProducerService, KafkaConsumerService, KafkaHealthIndicator, DEDUPE_STORE],
})
export class KafkaModule extends ConfigurableModuleClass {}
