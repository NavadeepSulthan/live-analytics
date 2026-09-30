import type { StoredEvent } from "../api/types";

export type FeedStatus =
  "connecting" | "live" | "reconnecting" | "unauthorized";

export type ToWorker =
  | { type: "connect"; baseUrl: string; readKey: string }
  | { type: "view"; windowSeconds: number; maxPoints: number }
  | { type: "disconnect" };

/** Chart-ready columns; arrays are transferred (zero-copy) from the worker. */
export interface ChartColumns {
  t: Float64Array;
  values: Float64Array[];
}

export interface LiveFrame {
  type: "frame";
  throughput: ChartColumns; // [requests/s, 5xx/s]
  latency: ChartColumns; // [p50, p95, p99]
  requestsPerSecond: number;
  errorRate: number;
  latestMs: number;
  newEvents: StoredEvent[];
  computeMs: number;
}

export type FromWorker =
  LiveFrame | { type: "status"; status: FeedStatus; detail?: string };
