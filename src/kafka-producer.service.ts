import {
  Injectable,
  Inject,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Kafka, Producer, CompressionTypes, logLevel } from 'kafkajs';
import { MODULE_OPTIONS_TOKEN } from './kafka.module-definition';
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
  private readonly kafka: Kafka;
  private readonly producer: Producer;

  constructor(
    @Inject(MODULE_OPTIONS_TOKEN)
    private readonly options: KafkaModuleOptions,
  ) {
    this.kafka = new Kafka({
      clientId: this.options.clientId,
      brokers: this.options.brokers,
      logLevel: LOG_LEVEL_MAP[this.options.logLevel ?? 'ERROR'],
      retry: {
        initialRetryTime: this.options.retry?.initialRetryTime ?? 300,
        retries: this.options.retry?.retries ?? 8,
      },
      ...(this.options.ssl !== undefined && { ssl: this.options.ssl }),
      ...(this.options.sasl && { sasl: this.options.sasl }),
    });

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
