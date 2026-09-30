export interface SeriesValues {
  values: Float64Array;
}

/** Values of every series at the sample nearest to `ms` (binary search on sorted `t`). */
export function valuesAt(
  t: Float64Array,
  series: readonly SeriesValues[],
  ms: number,
): { t: number; values: number[] } | null {
  if (t.length === 0) return null;
  let lo = 0;
  let hi = t.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (t[mid]! < ms) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && Math.abs(t[lo - 1]! - ms) < Math.abs(t[lo]! - ms)) lo--;
  return { t: t[lo]!, values: series.map((s) => s.values[lo]!) };
}
