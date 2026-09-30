import type { StoredEvent } from "../api/types";

/**
 * Bounded, de-duplicated buffer of recent events, newest first. The live stream re-sends a
 * short overlap window each second, so the same event can arrive several times.
 */
export class EventBuffer {
  private readonly capacity: number;
  private readonly seen = new Set<string>();
  private readonly order: string[] = [];

  constructor(capacity: number) {
    this.capacity = capacity;
  }

  /** Records events and returns only those not seen before, newest first. */
  add(events: readonly StoredEvent[]): StoredEvent[] {
    const fresh: StoredEvent[] = [];
    for (const e of events) {
      if (this.seen.has(e.eventId)) continue;
      this.seen.add(e.eventId);
      this.order.push(e.eventId);
      fresh.push(e);
    }
    while (this.order.length > this.capacity)
      this.seen.delete(this.order.shift()!);
    return fresh.sort(
      (a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp),
    );
  }
}
