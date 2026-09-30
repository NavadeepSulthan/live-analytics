const compact = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
});

export function formatCount(n: number): string {
  if (!Number.isFinite(n)) return "–";
  return Math.abs(n) < 1000
    ? String(Math.round(n * 10) / 10)
    : compact.format(n);
}

export function formatMs(ms: number): string {
  if (!Number.isFinite(ms)) return "–";
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms >= 100) return `${Math.round(ms)} ms`;
  return `${ms.toFixed(1)} ms`;
}

export function formatPercent(ratio: number): string {
  if (!Number.isFinite(ratio)) return "–";
  const pct = ratio * 100;
  return `${pct < 1 && pct > 0 ? pct.toFixed(2) : pct.toFixed(1)}%`;
}

export function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString([], { hour12: false });
}
