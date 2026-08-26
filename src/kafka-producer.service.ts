import {
  Injectable,
  Inject,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Kafka, Producer, CompressionTypes, logLevel } from 'kafkajs';
import { KAFKA_CLIENT } from './kafka.module-definition';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';
import { BaseKafkaEvent } from './events/base.event';

const LOG_LEVEL_MAP: Record<string, logLevel> = {
  NOTHING: logLevel.NOTHING,
  ERROR: logLevel.ERROR,
  WARN: logLevel.WARN,
  INFO: logLevel.INFO,
  DEBUG: logLevel.DEBUG,
};

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private readonly producer: Producer;

  constructor(
    @Inject(KAFKA_CLIENT)
    private readonly kafka: Kafka,
  ) {
    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
    });
  }

  async onModuleInit() {
    try {
      await this.producer.connect();
      this.logger.log('Kafka Producer connected successfully!');
    } catch (error) {
      this.logger.error('Failed to connect to Kafka Broker', error);
    }
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
    this.logger.log('Kafka Producer disconnected gracefully!');
  }

  async publish<T>(
    topic: string,
    key: string,
    event: BaseKafkaEvent<T>,
  ): Promise<void> {
    try {
      await this.producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages: [
          {
            key,
            value: JSON.stringify(event),
            headers: {
              eventId: event.eventId,
              source: event.source,
              timestamp: event.timestamp,
              ...(event.traceId && { traceId: event.traceId }),
            },
          },
        ],
      });

      this.logger.debug(
        `[Kafka] Event "${event.eventType}" -> Topic "${topic}" (Key: ${key})`,
      );
    } catch (error) {
      this.logger.error(`Failed to publish event to topic "${topic}"`, error);
    }
  }
}
