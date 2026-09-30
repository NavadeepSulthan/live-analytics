import { useMemo, useState } from "react";
import type { ServiceStats } from "../api/types";
import { formatCount, formatMs, formatPercent } from "../lib/format";

type SortKey = "service" | "requests" | "errorRate" | "p95" | "p99";

const value = (s: ServiceStats, key: SortKey): number | string =>
  key === "service"
    ? s.service
    : key === "requests"
      ? s.requests
      : key === "errorRate"
        ? s.errorRate
        : s.latencyMs[key];

export function ServicesTable({
  services,
  selected,
  onSelect,
}: {
  services: ServiceStats[];
  selected: string;
  onSelect(service: string): void;
}) {
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({
    key: "requests",
    desc: true,
  });
  const sorted = useMemo(
    () =>
      [...services].sort((a, b) => {
        const va = value(a, sort.key);
        const vb = value(b, sort.key);
        const cmp =
          typeof va === "string"
            ? va.localeCompare(vb as string)
            : va - (vb as number);
        return sort.desc ? -cmp : cmp;
      }),
    [services, sort],
  );
  const header = (key: SortKey, label: string, numeric = true) => (
    <th
      className={numeric ? "num" : ""}
      aria-sort={
        sort.key === key ? (sort.desc ? "descending" : "ascending") : "none"
      }
    >
      <button
        type="button"
        onClick={() =>
          setSort((s) => ({ key, desc: s.key === key ? !s.desc : true }))
        }
      >
        {label}
        {sort.key === key ? (sort.desc ? " ↓" : " ↑") : ""}
      </button>
    </th>
  );

  return (
    <section className="panel services">
      <header className="panel-header">
        <h2>Services</h2>
        <span className="muted">Click a row to filter events</span>
      </header>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              {header("service", "Service", false)}
              {header("requests", "Requests")}
              {header("errorRate", "Errors")}
              {header("p95", "p95")}
              {header("p99", "p99")}
            </tr>
          </thead>
          <tbody>
            {sorted.map((s) => (
              <tr
                key={s.service}
                className={s.service === selected ? "selected" : ""}
                onClick={() =>
                  onSelect(s.service === selected ? "" : s.service)
                }
              >
                <td>{s.service}</td>
                <td className="num">{formatCount(s.requests)}</td>
                <td
                  className={`num ${s.errorRate > 0.05 ? "bad" : s.errorRate > 0.01 ? "warn" : ""}`}
                >
                  {formatPercent(s.errorRate)}
                </td>
                <td className="num">{formatMs(s.latencyMs.p95)}</td>
                <td className="num">{formatMs(s.latencyMs.p99)}</td>
              </tr>
            ))}
            {sorted.length === 0 && (
              <tr>
                <td colSpan={5} className="muted">
                  No traffic in this window yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
