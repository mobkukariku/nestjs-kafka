import { KafkaHealthIndicator } from './kafka-health.indicator';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaConsumerService, ConsumerState } from './kafka-consumer.service';

function build(
  producerConnected: boolean,
  consumers: Record<string, ConsumerState>,
  ready = producerConnected && Object.values(consumers).every((s) => s === 'RUNNING'),
) {
  const producer = { isConnected: () => producerConnected } as KafkaProducerService;
  const consumer = {
    isReady: () => ready,
    getState: () => consumers,
  } as unknown as KafkaConsumerService;
  return new KafkaHealthIndicator(producer, consumer);
}

describe('KafkaHealthIndicator', () => {
  it('здоров, когда продюсер подключён и все группы в RUNNING', () => {
    expect(build(true, { g1: 'RUNNING', g2: 'RUNNING' }).check()).toEqual({
      ready: true,
      producer: 'connected',
      consumers: { g1: 'RUNNING', g2: 'RUNNING' },
    });
  });

  it('нездоров при отвалившемся продюсере', () => {
    expect(build(false, { g1: 'RUNNING' }).check()).toMatchObject({
      ready: false,
      producer: 'disconnected',
    });
  });

  it('нездоров при упавшей группе и показывает какой именно', () => {
    expect(build(true, { g1: 'RUNNING', g2: 'CRASHED' }).check()).toMatchObject({
      ready: false,
      consumers: { g2: 'CRASHED' },
    });
  });

  it('различает штатный STOPPED и аварийный CRASHED', () => {
    expect(build(true, { g1: 'STOPPED' }).check().consumers.g1).toBe('STOPPED');
  });
});
