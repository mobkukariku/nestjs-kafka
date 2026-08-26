import { SASLOptions } from 'kafkajs';
import { ConnectionOptions as TLSOptions } from 'tls';

export interface KafkaConsumerOptions {
  groupId: string;
  topics: string[];
  sessionTimeout?: number;
  rebalanceTimeout?: number;
  /** Max handler attempts per message before it is sent to the dead letter topic (default: 3). */
  maxRetries?: number;
  /** Topic to publish messages to once retries are exhausted or the message shape is invalid. If omitted, such messages are logged and dropped. */
  deadLetterTopic?: string;
}

export interface KafkaModuleOptions {
  clientId: string;
  brokers: string[];
  logLevel?: 'NOTHING' | 'ERROR' | 'WARN' | 'INFO' | 'DEBUG';
  retry?: {
    initialRetryTime?: number;
    retries?: number;
  };
  ssl?: boolean | TLSOptions;
  sasl?: SASLOptions;
  consumers?: KafkaConsumerOptions[];
}
