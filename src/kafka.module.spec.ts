import { DynamicModule, Provider } from '@nestjs/common';
import { KafkaModule } from './kafka.module';
import { MODULE_OPTIONS_TOKEN } from './kafka.module-definition';
import { DEDUPE_STORE } from './dedupe/dedupe.store';
import { InMemoryDedupeStore } from './dedupe/in-memory-dedupe.store';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';

const baseOptions: KafkaModuleOptions = { clientId: 'test', brokers: ['localhost:9092'] };

/** Фабрика провайдера берётся из метаданных @Module — без поднятия приложения. */
function dedupeFactory(): (options: KafkaModuleOptions) => unknown {
  const providers = Reflect.getMetadata('providers', KafkaModule) as Provider[];
  const provider = providers.find(
    (p): p is Extract<Provider, { provide: unknown; useFactory: unknown }> =>
      typeof p === 'object' && 'provide' in p && p.provide === DEDUPE_STORE,
  );
  return (provider as { useFactory: (o: KafkaModuleOptions) => unknown }).useFactory;
}

describe('KafkaModule', () => {
  it('по умолчанию регистрируется глобально', () => {
    const dyn = KafkaModule.forRoot(baseOptions) as DynamicModule;
    expect(dyn.global).toBe(true);
  });

  it('глобальность отключается через isGlobal', () => {
    const dyn = KafkaModule.forRoot({ ...baseOptions, isGlobal: false }) as DynamicModule;
    expect(dyn.global).toBe(false);
  });

  it('forRootAsync тоже уважает isGlobal', () => {
    const dyn = KafkaModule.forRootAsync({
      isGlobal: false,
      useFactory: () => baseOptions,
    }) as DynamicModule;
    expect(dyn.global).toBe(false);
  });

  it('пробрасывает опции под MODULE_OPTIONS_TOKEN', () => {
    const dyn = KafkaModule.forRoot(baseOptions) as DynamicModule;
    const provided = (dyn.providers ?? []).some(
      (p) => typeof p === 'object' && 'provide' in p && p.provide === MODULE_OPTIONS_TOKEN,
    );
    expect(provided).toBe(true);
  });

  it('по умолчанию подставляет InMemoryDedupeStore', () => {
    expect(dedupeFactory()(baseOptions)).toBeInstanceOf(InMemoryDedupeStore);
  });

  it('уважает переданный извне стор', () => {
    const custom = { claim: jest.fn(), commit: jest.fn(), release: jest.fn() };
    expect(dedupeFactory()({ ...baseOptions, dedupe: custom })).toBe(custom);
  });
});
