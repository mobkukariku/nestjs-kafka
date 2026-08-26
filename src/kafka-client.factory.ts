import { Kafka, logLevel } from "kafkajs";
import { KafkaModuleOptions } from "./types/kafka-module-options.interface";

const LOG_LEVEL_MAP: Record<string, logLevel> = {
  NOTHING: logLevel.NOTHING,
  ERROR: logLevel.ERROR,
  WARN: logLevel.WARN,
  INFO: logLevel.INFO,
  DEBUG: logLevel.DEBUG,
}

export function createKafkaClient(options: KafkaModuleOptions): Kafka {
    return new Kafka({
        clientId: options.clientId,
        brokers: options.brokers,
        logLevel: LOG_LEVEL_MAP[options.logLevel ?? 'ERROR'],
        retry: {
            initialRetryTime: options.retry?.initialRetryTime ?? 300,
            retries: options.retry?.retries ?? 8,
        },
        ...(options.ssl !== undefined && {ssl: options.ssl}),
        ...(options.sasl && { sasl: options.sasl }),
    });
}