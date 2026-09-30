import { useCallback, useEffect, useRef, useState } from "react";
import type { ApiClient } from "../api/client";
import type { StatusClass, StoredEvent } from "../api/types";

export interface LogFilter {
  service?: string;
  statusClass?: StatusClass;
}

const MAX_ROWS = 50_000;
const PAGE_SIZE = 500;

function matches(e: StoredEvent, f: LogFilter): boolean {
  if (f.service && e.service !== f.service) return false;
  if (f.statusClass && Math.floor(e.status / 100) !== Number(f.statusClass[0]))
    return false;
  return true;
}

interface LogState {
  rows: StoredEvent[];
  queued: StoredEvent[];
  loading: boolean;
  hasMore: boolean;
  error: string | null;
}

/**
 * Event log = live tail prepended to history (keyset-paged from the API). The owning
 * component is keyed by the filter, so a filter change remounts it with fresh state.
 * While `held` (paused, or scrolled down) new live rows queue up instead of shifting the
 * rows the user is reading.
 */
export function useEventLog(
  client: ApiClient,
  filter: LogFilter,
  held: boolean,
  subscribe: (listener: (events: StoredEvent[]) => void) => () => void,
) {
  const [state, setState] = useState<LogState>({
    rows: [],
    queued: [],
    loading: true,
    hasMore: true,
    error: null,
  });
  const cursor = useRef<string | undefined>(undefined);
  const ids = useRef(new Set<string>());
  const busy = useRef(false);
  const heldRef = useRef(held);

  useEffect(() => {
    heldRef.current = held;
  }, [held]);

  const fetchPage = useCallback(
    async (signal?: AbortSignal) => {
      busy.current = true;
      try {
        const page = await client.events(
          { ...filter, cursor: cursor.current, limit: PAGE_SIZE },
          signal,
        );
        const fresh = page.events.filter((e) => !ids.current.has(e.eventId));
        fresh.forEach((e) => ids.current.add(e.eventId));
        cursor.current = page.nextCursor;
        setState((s) => ({
          ...s,
          rows: [...s.rows, ...fresh].slice(0, MAX_ROWS),
          loading: false,
          hasMore: Boolean(page.nextCursor),
          error: null,
        }));
      } catch (err) {
        if ((err as Error).name !== "AbortError") {
          setState((s) => ({
            ...s,
            loading: false,
            error: (err as Error).message,
          }));
        }
      } finally {
        busy.current = false;
      }
    },
    [client, filter],
  );

  useEffect(() => {
    const controller = new AbortController();
    void fetchPage(controller.signal);
    return () => controller.abort();
  }, [fetchPage]);

  useEffect(
    () =>
      subscribe((events) => {
        const fresh = events.filter(
          (e) => matches(e, filter) && !ids.current.has(e.eventId),
        );
        if (fresh.length === 0) return;
        fresh.forEach((e) => ids.current.add(e.eventId));
        setState((s) =>
          heldRef.current
            ? { ...s, queued: [...fresh, ...s.queued].slice(0, MAX_ROWS) }
            : { ...s, rows: [...fresh, ...s.rows].slice(0, MAX_ROWS) },
        );
      }),
    [subscribe, filter],
  );

  /** Called from scroll handlers when the user nears the end of the loaded rows. */
  const loadMore = useCallback(() => {
    if (busy.current || !state.hasMore) return;
    setState((s) => ({ ...s, loading: true }));
    void fetchPage();
  }, [fetchPage, state.hasMore]);

  const showQueued = useCallback(() => {
    setState((s) => ({
      ...s,
      rows: [...s.queued, ...s.rows].slice(0, MAX_ROWS),
      queued: [],
    }));
  }, []);

  return { ...state, queued: state.queued.length, loadMore, showQueued };
}
