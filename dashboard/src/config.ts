import type { Connection } from "./api/client";

export const DEFAULT_QUERY_URL: string =
  import.meta.env.VITE_QUERY_URL ?? "http://localhost:8081";
/** Pre-filled only in dev builds so a read key is never baked into a production bundle. */
export const DEV_READ_KEY: string = import.meta.env.DEV
  ? (import.meta.env.VITE_READ_KEY ?? "")
  : "";

const STORAGE_KEY = "live-analytics:connection";

/** Tab-scoped storage: the key disappears when the tab closes. */
export const connectionStore = {
  load(): Connection | null {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      return raw ? (JSON.parse(raw) as Connection) : null;
    } catch {
      return null;
    }
  },
  save(conn: Connection) {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(conn));
  },
  clear() {
    sessionStorage.removeItem(STORAGE_KEY);
  },
};
