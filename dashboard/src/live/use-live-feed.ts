import { useEffect, useRef, useState } from "react";
import type { Connection } from "../api/client";
import type { StoredEvent } from "../api/types";
import type { FeedStatus, FromWorker, LiveFrame, ToWorker } from "./protocol";

interface LiveView {
  windowSeconds: number;
  maxPoints: number;
}

/**
 * Runs the live feed in a Web Worker. The main thread only receives chart-ready frames once a
 * second, so parsing and downsampling never compete with rendering or scrolling.
 */
export function useLiveFeed(
  conn: Connection,
  view: LiveView | null,
  onEvents: (events: StoredEvent[]) => void,
) {
  const [frame, setFrame] = useState<LiveFrame | null>(null);
  const [status, setStatus] = useState<{ status: FeedStatus; detail?: string }>(
    { status: "connecting" },
  );
  const workerRef = useRef<Worker | null>(null);
  const onEventsRef = useRef(onEvents);

  useEffect(() => {
    onEventsRef.current = onEvents;
  }, [onEvents]);

  useEffect(() => {
    const worker = new Worker(new URL("./live.worker.ts", import.meta.url), {
      type: "module",
    });
    workerRef.current = worker;
    worker.onmessage = (e: MessageEvent<FromWorker>) => {
      const msg = e.data;
      if (msg.type === "status") {
        setStatus({ status: msg.status, detail: msg.detail });
        return;
      }
      setFrame(msg);
      if (msg.newEvents.length > 0) onEventsRef.current(msg.newEvents);
    };
    const connect: ToWorker = {
      type: "connect",
      baseUrl: conn.baseUrl,
      readKey: conn.readKey,
    };
    worker.postMessage(connect);
    return () => {
      worker.postMessage({ type: "disconnect" } satisfies ToWorker);
      worker.terminate();
      workerRef.current = null;
    };
  }, [conn]);

  const windowSeconds = view?.windowSeconds;
  const maxPoints = view?.maxPoints;
  useEffect(() => {
    if (windowSeconds === undefined || maxPoints === undefined) return;
    workerRef.current?.postMessage({
      type: "view",
      windowSeconds,
      maxPoints,
    } satisfies ToWorker);
  }, [windowSeconds, maxPoints, conn]);

  return { frame, ...status };
}
