import type { Redis } from 'ioredis';
import { DedupeStore } from './dedupe.store';

const DEFAULT_TTL_SEC = 24 * 3600;
const DEFAULT_INFLIGHT_TTL_SEC = 5 * 60;

/**
 * Дедупликация в Redis — единственный вариант, дающий гарантию при нескольких
 * репликах сервиса. Требует установленного `ioredis` (optional peer dependency).
 */
export class RedisDedupeStore implements DedupeStore {
  constructor(
    private readonly redis: Redis,
    private readonly ttlSec = DEFAULT_TTL_SEC,
    private readonly prefix = 'evt:',
    private readonly inflightTtlSec = DEFAULT_INFLIGHT_TTL_SEC,
  ) {}

  async claim(eventId: string): Promise<boolean> {
    const res = await this.redis.set(this.key(eventId), '1', 'EX', this.inflightTtlSec, 'NX');
    return res === 'OK';
  }

  async commit(eventId: string): Promise<void> {
    // Именно SET, а не EXPIRE: если in-flight запись успела протухнуть, EXPIRE
    // по несуществующему ключу ничего не сделает и дубликат пройдёт повторно.
    await this.redis.set(this.key(eventId), '1', 'EX', this.ttlSec);
  }

  async release(eventId: string): Promise<void> {
    await this.redis.del(this.key(eventId));
  }

  private key(eventId: string): string {
    return this.prefix + eventId;
  }
}

/** @deprecated Опечатка в имени. Используйте `RedisDedupeStore`; алиас будет удалён в 3.0. */
export const RedisDedupStore = RedisDedupeStore;
