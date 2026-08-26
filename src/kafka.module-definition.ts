import { ConfigurableModuleBuilder } from '@nestjs/common';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';

export const {
  ConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN,
  OPTIONS_TYPE,
  ASYNC_OPTIONS_TYPE,
} = new ConfigurableModuleBuilder<KafkaModuleOptions>()
  .setClassMethodName('forRoot')
  .setFactoryMethodName('createKafkaModuleOptions')
  .build();

export const KAFKA_CLIENT = Symbol('KAFKA_CLIENT');
