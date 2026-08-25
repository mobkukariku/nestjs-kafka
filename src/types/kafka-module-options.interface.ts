import { SASLOptions } from 'kafkajs';
import { ConnectionOptions as TLSOptions } from 'tls';

export interface KafkaConsumerOptions {
  groupId: string;
  topics: string[];
  sessionTimeout?: number;
  rebalanceTimeout?: number;
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
  consumer?: KafkaConsumerOptions;
}
