import { useVirtualizer } from "@tanstack/react-virtual";
import { useRef, useState } from "react";
import type { ApiClient } from "../api/client";
import type { StatusClass, StoredEvent } from "../api/types";
import { useEventLog, type LogFilter } from "../hooks/use-event-log";
import { formatMs } from "../lib/format";

const ROW_HEIGHT = 28;
const LOAD_MORE_THRESHOLD_ROWS = 50;

interface LogTableProps {
  client: ApiClient;
  filter: LogFilter;
  services: string[];
  subscribe: (listener: (events: StoredEvent[]) => void) => () => void;
  onServiceChange(service: string): void;
  onStatusClassChange(statusClass: StatusClass | ""): void;
}

/**
 * Virtualized event log: only the ~30 visible rows exist in the DOM whether 100 or 50,000 are
 * loaded. Scrolling near the end fetches the next keyset page. Parents key this component by
 * filter, so changing a filter starts a fresh log.
 */
export function LogTable({
  client,
  filter,
  services,
  subscribe,
  onServiceChange,
  onStatusClassChange,
}: LogTableProps) {
  const [paused, setPaused] = useState(false);
  const [scrolledAway, setScrolledAway] = useState(false);
  const log = useEventLog(client, filter, paused || scrolledAway, subscribe);
  const scrollRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: log.rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });

  const onScroll = (e: React.UIEvent<HTMLDivElement>) => {
    const el = e.currentTarget;
    setScrolledAway(el.scrollTop > ROW_HEIGHT);
    if (
      el.scrollHeight - el.scrollTop - el.clientHeight <
      LOAD_MORE_THRESHOLD_ROWS * ROW_HEIGHT
    )
      log.loadMore();
  };

  const showQueued = () => {
    scrollRef.current?.scrollTo({ top: 0 });
    log.showQueued();
  };

  return (
    <section className="panel logs">
      <header className="panel-header">
        <h2>
          Events{" "}
          <span className="muted">
            {log.rows.length.toLocaleString()} loaded
          </span>
        </h2>
        <div className="controls">
          <select
            aria-label="Service filter"
            value={filter.service ?? ""}
            onChange={(e) => onServiceChange(e.target.value)}
          >
            <option value="">All services</option>
            {services.map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
          <select
            aria-label="Status filter"
            value={filter.statusClass ?? ""}
            onChange={(e) =>
              onStatusClassChange(e.target.value as StatusClass | "")
            }
          >
            <option value="">All statuses</option>
            {(["2xx", "3xx", "4xx", "5xx"] as const).map((c) => (
              <option key={c}>{c}</option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => {
              if (paused) log.showQueued();
              setPaused(!paused);
            }}
          >
            {paused ? "Resume live" : "Pause"}
          </button>
        </div>
      </header>
      {log.queued > 0 && (
        <button type="button" className="queued" onClick={showQueued}>
          {log.queued.toLocaleString()} new events. Show them.
        </button>
      )}
      <div className="log-row log-head" role="row">
        <span>Time</span>
        <span>Service</span>
        <span>Method</span>
        <span>Route</span>
        <span>Status</span>
        <span className="num">Duration</span>
        <span>Region</span>
      </div>
      <div
        ref={scrollRef}
        className="log-scroll"
        data-testid="log-scroll"
        onScroll={onScroll}
      >
        <div
          style={{ height: virtualizer.getTotalSize(), position: "relative" }}
        >
          {virtualizer.getVirtualItems().map((item) => {
            const e = log.rows[item.index]!;
            return (
              <div
                key={e.eventId}
                className="log-row"
                role="row"
                data-testid="log-row"
                style={{
                  transform: `translateY(${item.start}px)`,
                  height: ROW_HEIGHT,
                }}
              >
                <span className="mono">
                  {new Date(e.timestamp).toLocaleTimeString([], {
                    hour12: false,
                  })}
                </span>
                <span>{e.service}</span>
                <span className="mono">{e.method}</span>
                <span className="mono truncate" title={e.route}>
                  {e.route}
                </span>
                <span className={`status s${Math.floor(e.status / 100)}xx`}>
                  {e.status}
                </span>
                <span className="num mono">{formatMs(e.durationMs)}</span>
                <span className="muted">{e.region ?? ""}</span>
              </div>
            );
          })}
        </div>
        {log.loading && <div className="muted log-status">Loading…</div>}
        {!log.hasMore && log.rows.length > 0 && (
          <div className="muted log-status">End of the last 24 hours</div>
        )}
        {log.error && <div className="error log-status">{log.error}</div>}
      </div>
    </section>
  );
}
