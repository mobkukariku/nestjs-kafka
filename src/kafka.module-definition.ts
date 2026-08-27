import { ConfigurableModuleBuilder } from '@nestjs/common';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';

export interface KafkaModuleExtras {
  /**
   * Регистрировать модуль глобально. По умолчанию true — библиотека не должна
   * навязывать глобальную область, но менять поведение молча тоже нельзя.
   */
  isGlobal?: boolean;
}

export const {
  ConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN,
  OPTIONS_TYPE,
  ASYNC_OPTIONS_TYPE,
} = new ConfigurableModuleBuilder<KafkaModuleOptions>()
  .setClassMethodName('forRoot')
  .setFactoryMethodName('createKafkaModuleOptions')
  .setExtras<KafkaModuleExtras>({ isGlobal: true }, (definition, extras) => ({
    ...definition,
    global: extras.isGlobal,
  }))
  .build();

export const KAFKA_CLIENT = Symbol('KAFKA_CLIENT');
