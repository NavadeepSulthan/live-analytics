import { formatCount, formatMs, formatPercent } from "../lib/format";

export interface Kpis {
  requestsPerSecond: number | null;
  errorRate: number | null;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  queryMs: number | null;
}

function Card({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "bad" | "warn";
}) {
  return (
    <div
      className={`kpi ${tone ?? ""}`}
      data-testid={`kpi-${label.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`}
    >
      <span className="kpi-label">{label}</span>
      <span className="kpi-value">{value}</span>
      {hint && <span className="kpi-hint">{hint}</span>}
    </div>
  );
}

const show = (v: number | null, f: (n: number) => string) =>
  v === null ? "–" : f(v);

export function KpiCards({
  kpis,
  windowLabel,
}: {
  kpis: Kpis;
  windowLabel: string;
}) {
  const errorTone =
    kpis.errorRate === null
      ? undefined
      : kpis.errorRate > 0.05
        ? "bad"
        : kpis.errorRate > 0.01
          ? "warn"
          : undefined;
  return (
    <div className="kpis">
      <Card
        label="Requests / s"
        value={show(kpis.requestsPerSecond, formatCount)}
        hint={windowLabel}
      />
      <Card
        label="Error rate"
        value={show(kpis.errorRate, formatPercent)}
        hint="5xx share"
        tone={errorTone}
      />
      <Card label="p50 latency" value={show(kpis.p50, formatMs)} />
      <Card label="p95 latency" value={show(kpis.p95, formatMs)} />
      <Card label="p99 latency" value={show(kpis.p99, formatMs)} />
      <Card
        label="Query time"
        value={show(kpis.queryMs, formatMs)}
        hint="ClickHouse, server-side"
      />
    </div>
  );
}
