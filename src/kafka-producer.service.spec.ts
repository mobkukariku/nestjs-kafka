import { Logger } from '@nestjs/common';
import { CompressionTypes, Kafka } from 'kafkajs';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';
import { BaseKafkaEvent } from './events/base.event';

function build(options: Partial<KafkaModuleOptions> = {}) {
  const listeners: Record<string, () => void> = {};
  const send = jest.fn().mockResolvedValue(undefined);
  const producer = {
    events: { CONNECT: 'CONNECT', DISCONNECT: 'DISCONNECT' },
    on: jest.fn((event: string, cb: () => void) => { listeners[event] = cb; }),
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    send,
  };
  const producerFactory = jest.fn(() => producer);
  const kafka = { producer: producerFactory } as unknown as Kafka;

  const service = new KafkaProducerService(kafka, {
    clientId: 'test',
    brokers: ['localhost:9092'],
    ...options,
  });

  return { service, producer, producerFactory, send, emit: (e: string) => listeners[e]?.() };
}

function event(overrides: Partial<BaseKafkaEvent> = {}): BaseKafkaEvent {
  return {
    eventId: 'e-1',
    eventType: 'PAYMENT_PROCESSED',
    timestamp: '2026-08-27T00:00:00.000Z',
    source: 'billing',
    payload: { orderId: 'o-1' },
    ...overrides,
  };
}

beforeAll(() => {
  for (const level of ['log', 'warn', 'error', 'debug'] as const) {
    jest.spyOn(Logger.prototype, level).mockImplementation(() => undefined);
  }
});

describe('KafkaProducerService — конфигурация', () => {
  it('по умолчанию идемпотентный с maxInFlightRequests=1', () => {
    const { producerFactory } = build();
    expect(producerFactory).toHaveBeenCalledWith(
      expect.objectContaining({ idempotent: true, maxInFlightRequests: 1 }),
    );
  });

  it('уважает producer-опции', () => {
    const { producerFactory, send } = build({
      producer: { idempotent: false, maxInFlightRequests: 5, allowAutoTopicCreation: false },
    });
    expect(producerFactory).toHaveBeenCalledWith(
      expect.objectContaining({
        idempotent: false,
        maxInFlightRequests: 5,
        allowAutoTopicCreation: false,
      }),
    );
    expect(send).not.toHaveBeenCalled();
  });

  it('игнорирует maxInFlightRequests > 1 при idempotent и предупреждает', () => {
    const warn = jest.spyOn(Logger.prototype, 'warn');
    warn.mockClear();
    const { producerFactory } = build({ producer: { maxInFlightRequests: 5 } });
    expect(producerFactory).toHaveBeenCalledWith(
      expect.objectContaining({ idempotent: true, maxInFlightRequests: 1 }),
    );
    expect(warn).toHaveBeenCalled();
  });

  it('применяет acks и compression из опций', async () => {
    const { service, send } = build({
      producer: { acks: 1, compression: CompressionTypes.None },
    });
    await service.publish('t', 'k', event());
    expect(send).toHaveBeenCalledWith(
      expect.objectContaining({ acks: 1, compression: CompressionTypes.None }),
    );
  });

  it('по умолчанию acks=-1 и GZIP', async () => {
    const { service, send } = build();
    await service.publish('t', 'k', event());
    expect(send).toHaveBeenCalledWith(
      expect.objectContaining({ acks: -1, compression: CompressionTypes.GZIP }),
    );
  });
});

describe('KafkaProducerService — публикация', () => {
  it('проставляет version=1 по умолчанию согласованно в теле и заголовке', async () => {
    const { service, send } = build();
    await service.publish('t', 'k', event());

    const msg = send.mock.calls[0][0].messages[0];
    expect(JSON.parse(msg.value).version).toBe(1);
    expect(msg.headers.version).toBe('1');
  });

  it('сохраняет явную версию события', async () => {
    const { service, send } = build();
    await service.publish('t', 'k', event({ version: 3 }));

    const msg = send.mock.calls[0][0].messages[0];
    expect(JSON.parse(msg.value).version).toBe(3);
    expect(msg.headers.version).toBe('3');
  });

  // Регрессия: было JSON.stringify({version: 1, ...event}) — явный undefined
  // в событии затирал дефолт и расходился с заголовком.
  it('явный version: undefined не ломает тело сообщения', async () => {
    const { service, send } = build();
    await service.publish('t', 'k', event({ version: undefined }));

    const msg = send.mock.calls[0][0].messages[0];
    expect(JSON.parse(msg.value).version).toBe(1);
    expect(msg.headers.version).toBe('1');
  });

  it('кладёт traceId в заголовки только когда он есть', async () => {
    const { service, send } = build();
    await service.publish('t', 'k', event());
    expect(send.mock.calls[0][0].messages[0].headers).not.toHaveProperty('traceId');

    await service.publish('t', 'k', event({ traceId: 'tr-1' }));
    expect(send.mock.calls[1][0].messages[0].headers.traceId).toBe('tr-1');
  });

  it('пробрасывает ошибку отправки наружу', async () => {
    const { service, send } = build();
    send.mockRejectedValueOnce(new Error('брокер недоступен'));
    await expect(service.publish('t', 'k', event())).rejects.toThrow('брокер недоступен');
  });
});

describe('KafkaProducerService — жизненный цикл', () => {
  it('отслеживает подключение по событиям и onModuleInit', async () => {
    const { service, emit } = build();
    expect(service.isConnected()).toBe(false);

    await service.onModuleInit();
    expect(service.isConnected()).toBe(true);

    emit('DISCONNECT');
    expect(service.isConnected()).toBe(false);

    emit('CONNECT');
    expect(service.isConnected()).toBe(true);
  });

  // Продюсер обязан пережить остановку консьюмеров, иначе публикация в DLQ
  // из недообработанного батча упадёт на закрытом соединении.
  it('отключается в onApplicationShutdown, а не в onModuleDestroy', async () => {
    const { service, producer } = build();
    expect((service as unknown as Record<string, unknown>).onModuleDestroy).toBeUndefined();

    await service.onApplicationShutdown();
    expect(producer.disconnect).toHaveBeenCalled();
  });

  it('не бросает, если disconnect упал', async () => {
    const { service, producer } = build();
    producer.disconnect.mockRejectedValueOnce(new Error('уже закрыт'));
    await expect(service.onApplicationShutdown()).resolves.toBeUndefined();
  });
});
