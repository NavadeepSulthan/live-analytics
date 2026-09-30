import { useEffect, useRef, useState } from "react";

export interface PollState<T> {
  data: T | null;
  error: Error | null;
}

/**
 * Runs `fetcher` now and every `intervalMs` while `key` is unchanged, aborting in-flight
 * requests on change/unmount. Results are tagged with their key, so data for a previous key
 * is never shown; transient errors keep the last good data for the current key.
 */
export function usePoll<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  intervalMs: number,
  key: string,
): PollState<T> {
  const [state, setState] = useState<{
    key: string;
    data: T | null;
    error: Error | null;
  }>({ key, data: null, error: null });
  const fetcherRef = useRef(fetcher);

  useEffect(() => {
    fetcherRef.current = fetcher;
  }, [fetcher]);

  useEffect(() => {
    let controller: AbortController | null = null;
    let cancelled = false;
    const tick = async () => {
      controller?.abort();
      controller = new AbortController();
      try {
        const data = await fetcherRef.current(controller.signal);
        if (!cancelled) setState({ key, data, error: null });
      } catch (err) {
        if (!cancelled && (err as Error).name !== "AbortError") {
          setState((s) => ({
            key,
            data: s.key === key ? s.data : null,
            error: err as Error,
          }));
        }
      }
    };
    void tick();
    const timer = setInterval(() => void tick(), intervalMs);
    return () => {
      cancelled = true;
      clearInterval(timer);
      controller?.abort();
    };
  }, [intervalMs, key]);

  return state.key === key
    ? { data: state.data, error: state.error }
    : { data: null, error: null };
}
