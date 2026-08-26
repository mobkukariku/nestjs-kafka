import { Global, Module } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';
import { ConfigurableModuleClass, KAFKA_CLIENT, MODULE_OPTIONS_TOKEN } from './kafka.module-definition';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';
import { createKafkaClient } from './kafka-client.factory';
import { KafkaConsumerService } from './kafka-consumer.service';

@Global()
@Module({
  providers: [
    {
      provide: KAFKA_CLIENT,
      useFactory: (options: KafkaModuleOptions) => createKafkaClient(options),
      inject: [MODULE_OPTIONS_TOKEN],
    },
    KafkaProducerService,
    KafkaConsumerService,
  ],
  exports: [KafkaProducerService, KafkaConsumerService],
})
export class KafkaModule extends ConfigurableModuleClass {}
