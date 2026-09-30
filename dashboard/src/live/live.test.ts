import { describe, expect, it } from "vitest";
import type { LiveTick, StoredEvent } from "../api/types";
import { valuesAt } from "../charts/values-at";
import { formatCount, formatMs, formatPercent } from "../lib/format";
import { EventBuffer } from "./event-buffer";
import { lttbIndices, pick } from "./lttb";
import { SeriesStore } from "./series-store";
import { SseParser } from "./sse-parser";

describe("SseParser", () => {
  it("parses named events with multi-line data and ignores comments", () => {
    const parser = new SseParser();
    const messages = parser.push(
      "retry: 3000\n\n: ping\n\nevent: ticks\ndata: [1,\ndata: 2]\n\nevent: events\ndata: []\n\n",
    );
    expect(messages).toEqual([
      { event: "ticks", data: "[1,\n2]" },
      { event: "events", data: "[]" },
    ]);
  });

  it("handles messages split across arbitrary chunk boundaries, including CRLF", () => {
    const input = 'event: ticks\r\ndata: {"a":1}\r\n\r\ndata: x\n\n';
    for (let split = 1; split < input.length; split++) {
      const parser = new SseParser();
      const out = [
        ...parser.push(input.slice(0, split)),
        ...parser.push(input.slice(split)),
      ];
      expect(out).toEqual([
        { event: "ticks", data: '{"a":1}' },
        { event: "message", data: "x" },
      ]);
    }
  });
});

describe("lttbIndices", () => {
  it("returns every index when below the threshold", () => {
    expect(lttbIndices([0, 1, 2], [5, 6, 7], 10)).toEqual([0, 1, 2]);
  });

  it("keeps the endpoints and the spike when downsampling", () => {
    const n = 1000;
    const x = Array.from({ length: n }, (_, i) => i);
    const y = x.map((i) => (i === 537 ? 1000 : Math.sin(i / 50)));
    const idx = lttbIndices(x, y, 100);
    expect(idx).toHaveLength(100);
    expect(idx[0]).toBe(0);
    expect(idx.at(-1)).toBe(n - 1);
    expect(idx).toContain(537);
    expect(Math.max(...pick(y, idx))).toBe(1000);
  });
});

function tick(ts: number, requests: number, errors = 0, p95 = 10): LiveTick {
  return {
    ts: new Date(ts).toISOString(),
    requests,
    errors,
    latencyMs: { p50: p95 / 2, p95, p99: p95 * 2 },
    services: [],
  };
}

describe("SeriesStore", () => {
  const base = Date.UTC(2026, 8, 30, 12, 0, 0);

  it("upserts revised buckets instead of duplicating them", () => {
    const store = new SeriesStore(60);
    store.upsert([tick(base, 5)]);
    store.upsert([tick(base, 9), tick(base + 1000, 3)]);
    const cols = store.columns(base, base + 1000);
    expect(Array.from(cols.requests)).toEqual([9, 3]);
    expect(store.size).toBe(2);
  });

  it("zero-fills traffic and leaves latency gaps for missing seconds", () => {
    const store = new SeriesStore(60);
    store.upsert([tick(base, 4), tick(base + 2000, 6)]);
    const cols = store.columns(base, base + 2000);
    expect(Array.from(cols.requests)).toEqual([4, 0, 6]);
    expect(Number.isNaN(cols.p95[1])).toBe(true);
  });

  it("evicts buckets older than the retention window", () => {
    const store = new SeriesStore(10);
    store.upsert([tick(base, 1), tick(base + 20_000, 1)]);
    expect(store.size).toBe(1);
  });

  it("computes rates excluding the newest, still-filling second", () => {
    const store = new SeriesStore(60);
    store.upsert([
      tick(base, 10, 1),
      tick(base + 1000, 30, 2),
      tick(base + 2000, 999, 999),
    ]);
    expect(store.rates(2)).toEqual({ requestsPerSecond: 20, errorRate: 0.075 });
  });
});

describe("EventBuffer", () => {
  const event = (id: string, second: number): StoredEvent => ({
    eventId: id,
    timestamp: new Date(Date.UTC(2026, 0, 1, 0, 0, second)).toISOString(),
    receivedAt: new Date().toISOString(),
    service: "checkout",
    route: "/",
    method: "GET",
    status: 200,
    durationMs: 1,
  });

  it("returns only unseen events, newest first", () => {
    const buffer = new EventBuffer(100);
    expect(
      buffer.add([event("a", 1), event("b", 2)]).map((e) => e.eventId),
    ).toEqual(["b", "a"]);
    expect(
      buffer.add([event("b", 2), event("c", 3)]).map((e) => e.eventId),
    ).toEqual(["c"]);
  });

  it("forgets the oldest ids beyond capacity", () => {
    const buffer = new EventBuffer(2);
    buffer.add([event("a", 1), event("b", 2), event("c", 3)]);
    expect(buffer.add([event("a", 1)])).toHaveLength(1);
  });
});

describe("valuesAt", () => {
  it("returns the nearest sample for every series", () => {
    const t = Float64Array.from([1000, 2000, 3000]);
    const series = [
      { values: Float64Array.from([1, 2, 3]) },
      { values: Float64Array.from([10, 20, 30]) },
    ];
    expect(valuesAt(t, series, 2400)).toEqual({ t: 2000, values: [2, 20] });
    expect(valuesAt(t, series, 2600)).toEqual({ t: 3000, values: [3, 30] });
    expect(valuesAt(t, series, -5)).toEqual({ t: 1000, values: [1, 10] });
    expect(valuesAt(new Float64Array(), series, 0)).toBeNull();
  });
});

describe("format", () => {
  it("formats counts, durations and percentages", () => {
    expect(formatCount(950)).toBe("950");
    expect(formatCount(12_345)).toBe("12.3K");
    expect(formatMs(4.26)).toBe("4.3 ms");
    expect(formatMs(1500)).toBe("1.50 s");
    expect(formatPercent(0.0042)).toBe("0.42%");
    expect(formatPercent(0.15)).toBe("15.0%");
    expect(formatMs(Number.NaN)).toBe("–");
  });
});
