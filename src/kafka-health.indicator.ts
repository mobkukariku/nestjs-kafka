import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaConsumerService, ConsumerState } from './kafka-consumer.service';

export interface KafkaHealth {
  ready: boolean;
  producer: 'connected' | 'disconnected';
  consumers: Record<string, ConsumerState>;
}

@Injectable()
export class KafkaHealthIndicator {
  constructor(
    private readonly producer: KafkaProducerService,
    private readonly consumer: KafkaConsumerService,
  ) {}

  check(): KafkaHealth {
    return {
      ready: this.consumer.isReady(),
      producer: this.producer.isConnected() ? 'connected' : 'disconnected',
      consumers: this.consumer.getState(),
    };
  }
}