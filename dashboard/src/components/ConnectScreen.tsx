import { useState, type FormEvent } from "react";
import { ApiError, createClient, type Connection } from "../api/client";
import { DEFAULT_QUERY_URL, DEV_READ_KEY } from "../config";

export function ConnectScreen({
  onConnect,
}: {
  onConnect(conn: Connection): void;
}) {
  const [baseUrl, setBaseUrl] = useState(DEFAULT_QUERY_URL);
  const [readKey, setReadKey] = useState(DEV_READ_KEY);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setPending(true);
    setError(null);
    const conn = {
      baseUrl: baseUrl.replace(/\/$/, ""),
      readKey: readKey.trim(),
    };
    try {
      await createClient(conn).summary(60);
      onConnect(conn);
    } catch (err) {
      setError(
        err instanceof ApiError && err.status === 401
          ? "That read key was rejected."
          : `Could not reach ${conn.baseUrl}. Is the query service running?`,
      );
    } finally {
      setPending(false);
    }
  };

  return (
    <main className="connect">
      <form className="panel connect-card" onSubmit={submit}>
        <h1>Live Analytics</h1>
        <p className="muted">
          Connect to a query service with a tenant read key.
        </p>
        <label>
          Query service URL
          <input
            value={baseUrl}
            onChange={(e) => setBaseUrl(e.target.value)}
            required
          />
        </label>
        <label>
          Read API key
          <input
            value={readKey}
            onChange={(e) => setReadKey(e.target.value)}
            type="password"
            required
            autoComplete="off"
          />
        </label>
        {error && (
          <p role="alert" className="error">
            {error}
          </p>
        )}
        <button type="submit" disabled={pending}>
          {pending ? "Connecting…" : "Connect"}
        </button>
      </form>
    </main>
  );
}
