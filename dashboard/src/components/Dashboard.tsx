import { useCallback, useMemo, useState } from "react";
import { createClient, type Connection } from "../api/client";
import type { StatusClass, StoredEvent, TimeSeries } from "../api/types";
import { TimeChart, type ChartSeries } from "../charts/TimeChart";
import { usePoll } from "../hooks/use-poll";
import { createEmitter } from "../lib/emitter";
import { formatCount, formatMs } from "../lib/format";
import { useLiveFeed } from "../live/use-live-feed";
import { KpiCards, type Kpis } from "./KpiCards";
import { LogTable } from "./LogTable";
import { ServicesTable } from "./ServicesTable";

interface Range {
  id: string;
  label: string;
  seconds: number;
  live: boolean;
}

const RANGES: Range[] = [
  { id: "live-1m", label: "Live 1m", seconds: 60, live: true },
  { id: "live-5m", label: "Live 5m", seconds: 300, live: true },
  { id: "live-15m", label: "Live 15m", seconds: 900, live: true },
  { id: "1h", label: "1h", seconds: 3600, live: false },
  { id: "24h", label: "24h", seconds: 86_400, live: false },
  { id: "7d", label: "7d", seconds: 604_800, live: false },
];

const COLORS = {
  requests: "#38bdf8",
  errors: "#f87171",
  p50: "#a3e635",
  p95: "#fbbf24",
  p99: "#f472b6",
};
const LIVE_LAG_MS = 1500;

export function Dashboard({
  conn,
  onDisconnect,
}: {
  conn: Connection;
  onDisconnect(): void;
}) {
  const client = useMemo(() => createClient(conn), [conn]);
  const [range, setRange] = useState<Range>(RANGES[1]!);
  const [chartWidth, setChartWidth] = useState(600);
  const [hoverMs, setHoverMs] = useState<number | null>(null);
  const [service, setService] = useState("");
  const [statusClass, setStatusClass] = useState<StatusClass | "">("");
  const [liveEvents] = useState(() => createEmitter<StoredEvent[]>());

  const filter = useMemo(
    () => ({
      service: service || undefined,
      statusClass: statusClass || undefined,
    }),
    [service, statusClass],
  );
  const liveView = useMemo(
    () =>
      range.live
        ? { windowSeconds: range.seconds, maxPoints: chartWidth }
        : null,
    [range, chartWidth],
  );
  const live = useLiveFeed(conn, liveView, liveEvents.emit);

  const pollMs = range.live ? 5000 : 30_000;
  const summary = usePoll(
    (s) => client.summary(range.seconds, s),
    pollMs,
    `summary-${range.id}`,
  );
  const services = usePoll(
    (s) => client.services(range.seconds, s),
    pollMs,
    `services-${range.id}`,
  );
  const history = usePoll<TimeSeries | null>(
    (s) =>
      range.live ? Promise.resolve(null) : client.timeSeries(range.seconds, s),
    30_000,
    `history-${range.id}`,
  );

  const charts = useMemo(() => {
    if (range.live) {
      if (!live.frame) return null;
      const { throughput, latency } = live.frame;
      return {
        throughputT: throughput.t,
        throughput: throughput.values,
        latencyT: latency.t,
        latency: latency.values,
      };
    }
    const points = history.data?.points ?? [];
    const step = history.data?.stepSeconds ?? 1;
    const col = (f: (p: (typeof points)[number]) => number) =>
      Float64Array.from(points, f);
    const t = col((p) => Date.parse(p.ts));
    const gap = (v: number, p: (typeof points)[number]) =>
      p.requests > 0 ? v : Number.NaN;
    return {
      throughputT: t,
      throughput: [col((p) => p.requests / step), col((p) => p.errors / step)],
      latencyT: t,
      latency: [
        col((p) => gap(p.latencyMs.p50, p)),
        col((p) => gap(p.latencyMs.p95, p)),
        col((p) => gap(p.latencyMs.p99, p)),
      ],
    };
  }, [range.live, live.frame, history.data]);

  const throughputSeries: ChartSeries[] = charts
    ? [
        {
          label: "requests/s",
          color: COLORS.requests,
          values: charts.throughput[0]!,
          fill: true,
        },
        { label: "5xx/s", color: COLORS.errors, values: charts.throughput[1]! },
      ]
    : [];
  const latencySeries: ChartSeries[] = charts
    ? [
        { label: "p50", color: COLORS.p50, values: charts.latency[0]! },
        { label: "p95", color: COLORS.p95, values: charts.latency[1]! },
        { label: "p99", color: COLORS.p99, values: charts.latency[2]! },
      ]
    : [];
  const follow = range.live
    ? { windowMs: range.seconds * 1000, lagMs: LIVE_LAG_MS }
    : undefined;

  const kpis: Kpis = {
    requestsPerSecond: range.live
      ? (live.frame?.requestsPerSecond ?? null)
      : (summary.data?.requestsPerSecond ?? null),
    errorRate: range.live
      ? (live.frame?.errorRate ?? null)
      : (summary.data?.errorRate ?? null),
    p50: summary.data?.latencyMs.p50 ?? null,
    p95: summary.data?.latencyMs.p95 ?? null,
    p99: summary.data?.latencyMs.p99 ?? null,
    queryMs: summary.data?.queryMs ?? null,
  };

  const onWidth = useCallback(
    (w: number) => setChartWidth(Math.max(50, Math.round(w))),
    [],
  );
  const selectService = useCallback((s: string) => setService(s), []);
  const statusLabel = range.live ? live.status : "historical";

  return (
    <div className="dashboard">
      <header className="topbar">
        <h1>Live Analytics</h1>
        <span
          className={`pill ${statusLabel}`}
          data-testid="feed-status"
          title={live.detail}
        >
          ● {statusLabel}
        </span>
        <nav className="ranges" aria-label="Time range">
          {RANGES.map((r) => (
            <button
              key={r.id}
              type="button"
              className={r.id === range.id ? "active" : ""}
              onClick={() => setRange(r)}
            >
              {r.label}
            </button>
          ))}
        </nav>
        <span
          className="muted small"
          title="Time the Web Worker spent building the last chart frame"
        >
          {live.frame &&
            `worker ${Math.round(live.frame.computeMs * 1000)} µs/frame`}
        </span>
        <button type="button" className="ghost" onClick={onDisconnect}>
          Disconnect
        </button>
      </header>

      {live.status === "unauthorized" && (
        <div className="banner error" role="alert">
          The read key was rejected.{" "}
          <button onClick={onDisconnect}>Reconnect</button>
        </div>
      )}
      {(summary.error || services.error) && (
        <div className="banner" role="status">
          Query API error: {(summary.error ?? services.error)!.message}. Showing
          last known data.
        </div>
      )}

      <KpiCards
        kpis={kpis}
        windowLabel={range.live ? "last 10 s" : `avg over ${range.label}`}
      />

      <div className="charts">
        <TimeChart
          title="Throughput"
          t={charts?.throughputT ?? new Float64Array()}
          series={throughputSeries}
          follow={follow}
          format={formatCount}
          hoverMs={hoverMs}
          onHover={setHoverMs}
          onWidth={onWidth}
        />
        <TimeChart
          title="Latency"
          t={charts?.latencyT ?? new Float64Array()}
          series={latencySeries}
          follow={follow}
          format={formatMs}
          hoverMs={hoverMs}
          onHover={setHoverMs}
        />
      </div>

      <div className="bottom">
        <ServicesTable
          services={services.data ?? []}
          selected={service}
          onSelect={selectService}
        />
        <LogTable
          key={`${service}|${statusClass}`}
          client={client}
          filter={filter}
          services={(services.data ?? []).map((s) => s.service).sort()}
          subscribe={liveEvents.subscribe}
          onServiceChange={setService}
          onStatusClassChange={setStatusClass}
        />
      </div>
    </div>
  );
}
