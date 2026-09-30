/// <reference lib="webworker" />
import { createTraceparent } from "../api/trace";
import type { LiveTick, StoredEvent } from "../api/types";
import { EventBuffer } from "./event-buffer";
import { lttbIndices, pick } from "./lttb";
import type {
  ChartColumns,
  FeedStatus,
  FromWorker,
  LiveFrame,
  ToWorker,
} from "./protocol";
import { SeriesStore } from "./series-store";
import { SseParser, type SseMessage } from "./sse-parser";

const RETENTION_SECONDS = 15 * 60;
const RATE_WINDOW_SECONDS = 10;
const WATCHDOG_MS = 30_000; // server pings every 15 s

const scope = self as unknown as DedicatedWorkerGlobalScope;
const store = new SeriesStore(RETENTION_SECONDS);
const buffer = new EventBuffer(20_000);
let view = { windowSeconds: 300, maxPoints: 600 };
let pendingEvents: StoredEvent[] = [];
let controller: AbortController | null = null;
let stopped = false;

scope.onmessage = (e: MessageEvent<ToWorker>) => {
  const msg = e.data;
  switch (msg.type) {
    case "connect":
      stopped = false;
      void run(msg.baseUrl, msg.readKey);
      break;
    case "view":
      view = {
        windowSeconds: msg.windowSeconds,
        maxPoints: Math.max(10, msg.maxPoints),
      };
      if (store.size > 0) emit();
      break;
    case "disconnect":
      stopped = true;
      controller?.abort();
      break;
  }
};

function post(message: FromWorker, transfer: Transferable[] = []) {
  scope.postMessage(message, transfer);
}

function status(s: FeedStatus, detail?: string) {
  post({ type: "status", status: s, detail });
}

/** Connects, reads the stream, and reconnects with capped exponential backoff plus jitter. */
async function run(baseUrl: string, readKey: string) {
  let attempt = 0;
  while (!stopped) {
    controller = new AbortController();
    status(attempt === 0 ? "connecting" : "reconnecting");
    try {
      const res = await fetch(`${baseUrl}/v1/stream`, {
        headers: {
          Authorization: `Bearer ${readKey}`,
          traceparent: createTraceparent(),
        },
        signal: controller.signal,
      });
      if (res.status === 401) {
        status("unauthorized", "The read key was rejected");
        return;
      }
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      attempt = 0;
      status("live");
      await consume(res.body, controller);
    } catch {
      // Network errors and watchdog aborts fall through to reconnect.
    }
    if (stopped) return;
    attempt++;
    const delay =
      Math.min(30_000, 1000 * 2 ** Math.min(attempt, 5)) *
      (0.5 + Math.random() / 2);
    status("reconnecting", `retrying in ${Math.round(delay / 1000)}s`);
    await new Promise((r) => setTimeout(r, delay));
  }
}

async function consume(
  body: ReadableStream<Uint8Array>,
  abort: AbortController,
) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  const parser = new SseParser();
  let watchdog = setTimeout(() => abort.abort(), WATCHDOG_MS);
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      clearTimeout(watchdog);
      watchdog = setTimeout(() => abort.abort(), WATCHDOG_MS);
      for (const message of parser.push(
        decoder.decode(value, { stream: true }),
      ))
        handle(message);
    }
  } finally {
    clearTimeout(watchdog);
  }
}

function handle(message: SseMessage) {
  if (message.event === "ticks") {
    store.upsert(JSON.parse(message.data) as LiveTick[]);
  } else if (message.event === "events") {
    pendingEvents.push(
      ...buffer.add(JSON.parse(message.data) as StoredEvent[]),
    );
    // The server sends ticks then events once per second: emit one frame per update.
    emit();
  }
}

/** Builds chart columns for the visible window, downsampled to at most one point per pixel. */
function emit() {
  const started = performance.now();
  // The newest bucket is still filling; show up to the one before it.
  const end = store.latest - 1000;
  const cols = store.columns(end - view.windowSeconds * 1000, end);

  const throughputIdx = lttbIndices(cols.t, cols.requests, view.maxPoints);
  const latencyIdx = lttbIndices(cols.t, cols.p99, view.maxPoints);
  const throughput: ChartColumns = {
    t: pick(cols.t, throughputIdx),
    values: [
      pick(cols.requests, throughputIdx),
      pick(cols.errors, throughputIdx),
    ],
  };
  const latency: ChartColumns = {
    t: pick(cols.t, latencyIdx),
    values: [
      pick(cols.p50, latencyIdx),
      pick(cols.p95, latencyIdx),
      pick(cols.p99, latencyIdx),
    ],
  };
  const rates = store.rates(RATE_WINDOW_SECONDS);

  const frame: LiveFrame = {
    type: "frame",
    throughput,
    latency,
    requestsPerSecond: rates.requestsPerSecond,
    errorRate: rates.errorRate,
    latestMs: end,
    newEvents: pendingEvents,
    computeMs: performance.now() - started,
  };
  pendingEvents = [];
  const transfer = [
    throughput.t,
    ...throughput.values,
    latency.t,
    ...latency.values,
  ].map((a) => a.buffer);
  post(frame, transfer);
}
