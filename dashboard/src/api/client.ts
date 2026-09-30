import { createTraceparent } from "./trace";
import type {
  ApiErrorBody,
  EventPage,
  ServiceStats,
  StatusClass,
  Summary,
  TimeSeries,
} from "./types";

export interface Connection {
  baseUrl: string;
  readKey: string;
}

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

export interface EventQuery {
  service?: string;
  statusClass?: StatusClass;
  cursor?: string;
  limit?: number;
}

type Params = Record<string, string | number | undefined>;

/** Typed client for the query API; response types come from the shared OpenAPI contract. */
export function createClient(conn: Connection) {
  async function get<T>(
    path: string,
    params: Params,
    signal?: AbortSignal,
  ): Promise<T> {
    const url = new URL(path, conn.baseUrl);
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== "")
        url.searchParams.set(key, String(value));
    }
    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${conn.readKey}`,
        traceparent: createTraceparent(),
      },
      signal,
    });
    if (!res.ok) {
      const body = (await res.json().catch(() => null)) as ApiErrorBody | null;
      throw new ApiError(
        res.status,
        body?.error.code ?? "http_error",
        body?.error.message ?? `HTTP ${res.status}`,
      );
    }
    return (await res.json()) as T;
  }

  return {
    summary: (windowSeconds: number, signal?: AbortSignal) =>
      get<Summary>("/v1/metrics/summary", { windowSeconds }, signal),
    timeSeries: (windowSeconds: number, signal?: AbortSignal) =>
      get<TimeSeries>("/v1/metrics/timeseries", { windowSeconds }, signal),
    services: (windowSeconds: number, signal?: AbortSignal) =>
      get<ServiceStats[]>("/v1/metrics/services", { windowSeconds }, signal),
    events: (query: EventQuery, signal?: AbortSignal) =>
      get<EventPage>("/v1/events", { ...query }, signal),
  };
}

export type ApiClient = ReturnType<typeof createClient>;
