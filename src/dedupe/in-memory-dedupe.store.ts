import { Logger } from '@nestjs/common';
import { DedupeStore } from './dedupe.store';

const DEFAULT_TTL_MS = 24 * 3600_000;
const DEFAULT_INFLIGHT_TTL_MS = 5 * 60_000;
const DEFAULT_MAX_SIZE = 100_000;

/**
 * Дедупликация в памяти процесса.
 *
 * ВАЖНО: стор per-process. При нескольких репликах сервиса каждая реплика ведёт
 * собственный набор eventId, поэтому одно и то же событие может быть обработано
 * по разу на каждой реплике. Для мультиреплики используйте `RedisDedupeStore`
 * из `@mobkukariku/nestjs-kafka/redis`.
 */
export class InMemoryDedupeStore implements DedupeStore {
  private readonly logger = new Logger(InMemoryDedupeStore.name);
  /** eventId -> момент истечения (ms epoch). Порядок вставки ~ порядок истечения. */
  private readonly seen = new Map<string, number>();

  constructor(
    private readonly ttlMs = DEFAULT_TTL_MS,
    private readonly maxSize = DEFAULT_MAX_SIZE,
    private readonly inflightTtlMs = DEFAULT_INFLIGHT_TTL_MS,
  ) {}

  claim(eventId: string): Promise<boolean> {
    const expiresAt = this.seen.get(eventId);
    // Протухшая запись не блокирует: проверяем срок явно, не полагаясь на evict().
    if (expiresAt !== undefined && expiresAt > Date.now()) {
      return Promise.resolve(false);
    }

    this.seen.set(eventId, Date.now() + this.inflightTtlMs);
    // Уборка после вставки, иначе фактический потолок был бы maxSize + 1.
    this.evict();
    return Promise.resolve(true);
  }

  commit(eventId: string): Promise<void> {
    // delete + set переставляет запись в хвост, чтобы порядок вставки
    // оставался согласован с порядком истечения — на нём держится evict().
    this.seen.delete(eventId);
    this.seen.set(eventId, Date.now() + this.ttlMs);
    return Promise.resolve();
  }

  release(eventId: string): Promise<void> {
    this.seen.delete(eventId);
    return Promise.resolve();
  }

  /** Только для тестов и диагностики. */
  size(): number {
    return this.seen.size;
  }

  private evict(): void {
    const now = Date.now();

    // Просроченные записи скапливаются в голове Map — выметаем их, пока не упрёмся в живую.
    for (const [id, expiresAt] of this.seen) {
      if (expiresAt > now) break;
      this.seen.delete(id);
    }

    if (this.seen.size <= this.maxSize) return;

    // Переполнение: вытесняем самые старые ЖИВЫЕ записи. Это уже нарушение
    // гарантии дедупликации, поэтому оно должно быть видно в логах, а не молча.
    const overflow = this.seen.size - this.maxSize;
    let removed = 0;
    for (const id of this.seen.keys()) {
      if (removed >= overflow) break;
      this.seen.delete(id);
      removed++;
    }

    this.logger.warn(
      `InMemoryDedupeStore переполнен (maxSize=${this.maxSize}): вытеснено ${removed} ` +
        'непросроченных записей, гарантия дедупликации нарушена. ' +
        'Увеличьте maxSize или перейдите на RedisDedupeStore.',
    );
  }
}
