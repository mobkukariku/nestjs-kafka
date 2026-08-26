import {
  Injectable,
  Inject,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import { KAFKA_CLIENT, MODULE_OPTIONS_TOKEN } from './kafka.module-definition';
import { KafkaEventHandler } from './types/kafka-event-handler.type';
import { KafkaConsumerOptions, KafkaModuleOptions } from './types/kafka-module-options.interface';
import { BaseKafkaEvent, isBaseKafkaEvent } from './events/base.event';
import { KafkaProducerService } from './kafka-producer.service';

const DEFAULT_MAX_RETRIES = 3;
const RETRY_DELAY_MS = 200;

interface ManagedConsumer {
    consumer: Consumer;
    options: KafkaConsumerOptions;
}

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnApplicationBootstrap, OnModuleDestroy {
    private readonly logger = new Logger(KafkaConsumerService.name);
    private readonly managedConsumers = new Map<string, ManagedConsumer>();
    private readonly handlers = new Map<string, KafkaEventHandler>();

    constructor(
        @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
        @Inject(MODULE_OPTIONS_TOKEN) private readonly options: KafkaModuleOptions,
        private readonly producer: KafkaProducerService,
    ){
        for (const consumerOptions of this.options.consumers ?? []) {
            if (this.managedConsumers.has(consumerOptions.groupId)) {
                throw new Error(`Duplicate Kafka consumer groupId "${consumerOptions.groupId}"`);
            }

            this.managedConsumers.set(consumerOptions.groupId, {
                consumer: this.kafka.consumer({
                    groupId: consumerOptions.groupId,
                    sessionTimeout: consumerOptions.sessionTimeout,
                    rebalanceTimeout: consumerOptions.rebalanceTimeout,
                }),
                options: consumerOptions,
            });
        }
    }

    registerHandler(eventType: string, handler: KafkaEventHandler): void {
        this.handlers.set(eventType, handler);
    }

    async onModuleInit() {
        if (this.managedConsumers.size === 0) {
            this.logger.warn('No Kafka consumers configured, skipping consumer start');
            return;
        }

        for (const { consumer, options } of this.managedConsumers.values()) {
            try {
                await consumer.connect();
                await consumer.subscribe({ topics: options.topics });
            } catch(error) {
                this.logger.error(`Failed to connect Kafka consumer "${options.groupId}"`, error);
            }
        }
    }

    async onApplicationBootstrap() {
        for (const { consumer, options } of this.managedConsumers.values()) {
            try {
                await consumer.run({
                    eachMessage: async (payload: EachMessagePayload) => {
                        await this.handleMessage(payload, options);
                    }
                });

                this.logger.log(`Kafka consumer "${options.groupId}" connected and running!`);
            } catch(error) {
                this.logger.error(`Failed to start Kafka consumer "${options.groupId}"`, error);
            }
        }
    }

    async onModuleDestroy() {
        for (const { consumer, options } of this.managedConsumers.values()) {
            await consumer.disconnect();
            this.logger.log(`Kafka consumer "${options.groupId}" disconnected gracefully`);
        }
    }

    private async handleMessage(payload: EachMessagePayload, options: KafkaConsumerOptions): Promise<void> {
        const raw = payload.message.value?.toString();
        if(!raw) return;

        let parsed: unknown;

        try {
            parsed = JSON.parse(raw);
        } catch(error) {
            this.logger.error(`Failed to parse Kafka message on topic "${payload.topic}"`, error);
            await this.sendToDeadLetter(payload, options, raw, 'Invalid JSON payload');
            return;
        }

        if (!isBaseKafkaEvent(parsed)) {
            this.logger.error(`Message on topic "${payload.topic}" does not match BaseKafkaEvent shape`);
            await this.sendToDeadLetter(payload, options, raw, 'Invalid event shape');
            return;
        }

        const event = parsed;
        const handler = this.handlers.get(event.eventType);
        if(!handler){
            this.logger.warn(`No handler registered for event type "${event.eventType}"`);
            return;
        }

        const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                await handler(event, payload);
                return;
            } catch(error) {
                this.logger.error(
                    `Handler failed for event "${event.eventType}" (attempt ${attempt}/${maxRetries})`,
                    error,
                );

                if (attempt < maxRetries) {
                    await this.sleep(RETRY_DELAY_MS * attempt);
                }
            }
        }

        await this.sendToDeadLetter(
            payload,
            options,
            raw,
            `Handler exhausted ${maxRetries} retries for event "${event.eventType}"`,
        );
    }

    private async sendToDeadLetter(
        payload: EachMessagePayload,
        options: KafkaConsumerOptions,
        raw: string,
        reason: string,
    ): Promise<void> {
        if (!options.deadLetterTopic) {
            this.logger.warn(`No deadLetterTopic configured for group "${options.groupId}", dropping message`);
            return;
        }

        const dlqEvent: BaseKafkaEvent<{
            originalTopic: string;
            originalPartition: number;
            originalOffset: string;
            raw: string;
            reason: string;
        }> = {
            eventId: randomUUID(),
            eventType: 'DEAD_LETTER',
            timestamp: new Date().toISOString(),
            source: options.groupId,
            payload: {
                originalTopic: payload.topic,
                originalPartition: payload.partition,
                originalOffset: payload.message.offset,
                raw,
                reason,
            },
        };

        try {
            await this.producer.publish(
                options.deadLetterTopic,
                payload.message.key?.toString() ?? payload.message.offset,
                dlqEvent,
            );
        } catch(error) {
            this.logger.error(`Failed to publish message to dead letter topic "${options.deadLetterTopic}"`, error);
        }
    }

    private sleep(ms: number): Promise<void> {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
