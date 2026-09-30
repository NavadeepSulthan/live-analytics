import type { LiveTick } from "../api/types";

/** Column-oriented series ready for charting (and transferable to the main thread). */
export interface SeriesColumns {
  t: Float64Array;
  requests: Float64Array;
  errors: Float64Array;
  p50: Float64Array;
  p95: Float64Array;
  p99: Float64Array;
}

interface Bucket {
  requests: number;
  errors: number;
  p50: number;
  p95: number;
  p99: number;
}

/**
 * Rolling store of one-second buckets keyed by timestamp. The live stream re-sends the last
 * 10 seconds on every update because late events revise recent buckets, so writes are upserts.
 */
export class SeriesStore {
  private readonly buckets = new Map<number, Bucket>();
  private readonly retentionMs: number;
  private latestMs = 0;

  constructor(retentionSeconds: number) {
    this.retentionMs = retentionSeconds * 1000;
  }

  get size(): number {
    return this.buckets.size;
  }

  get latest(): number {
    return this.latestMs;
  }

  upsert(ticks: readonly LiveTick[]): void {
    for (const tick of ticks) {
      const ms = Date.parse(tick.ts);
      if (Number.isNaN(ms)) continue;
      this.buckets.set(ms, {
        requests: tick.requests,
        errors: tick.errors,
        p50: tick.latencyMs.p50,
        p95: tick.latencyMs.p95,
        p99: tick.latencyMs.p99,
      });
      this.latestMs = Math.max(this.latestMs, ms);
    }
    const cutoff = this.latestMs - this.retentionMs;
    for (const ms of this.buckets.keys())
      if (ms < cutoff) this.buckets.delete(ms);
  }

  /** One row per second in [fromMs, toMs]; missing seconds get 0 traffic and NaN latency (a gap). */
  columns(fromMs: number, toMs: number): SeriesColumns {
    const start = Math.ceil(fromMs / 1000) * 1000;
    const n = Math.max(0, Math.floor((toMs - start) / 1000) + 1);
    const cols: SeriesColumns = {
      t: new Float64Array(n),
      requests: new Float64Array(n),
      errors: new Float64Array(n),
      p50: new Float64Array(n),
      p95: new Float64Array(n),
      p99: new Float64Array(n),
    };
    for (let i = 0; i < n; i++) {
      const ms = start + i * 1000;
      const b = this.buckets.get(ms);
      cols.t[i] = ms;
      cols.requests[i] = b?.requests ?? 0;
      cols.errors[i] = b?.errors ?? 0;
      cols.p50[i] = b && b.requests > 0 ? b.p50 : Number.NaN;
      cols.p95[i] = b && b.requests > 0 ? b.p95 : Number.NaN;
      cols.p99[i] = b && b.requests > 0 ? b.p99 : Number.NaN;
    }
    return cols;
  }

  /** Requests and errors per second averaged over the trailing window, skipping the newest (still filling) second. */
  rates(windowSeconds: number): {
    requestsPerSecond: number;
    errorRate: number;
  } {
    let requests = 0;
    let errors = 0;
    const end = this.latestMs - 1000;
    for (let ms = end - (windowSeconds - 1) * 1000; ms <= end; ms += 1000) {
      const b = this.buckets.get(ms);
      if (b) {
        requests += b.requests;
        errors += b.errors;
      }
    }
    return {
      requestsPerSecond: requests / windowSeconds,
      errorRate: requests > 0 ? errors / requests : 0,
    };
  }
}
