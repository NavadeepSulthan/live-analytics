/**
 * Largest-Triangle-Three-Buckets downsampling (Steinarsson, 2013). Keeps the visual shape of
 * a series (peaks and dips survive) while reducing it to `threshold` points, so a chart never
 * draws more points than it has pixels. Returns the indices to keep, always including the
 * first and last. NaN y-values (gaps) are treated as 0 when scoring but can still be kept.
 */
export function lttbIndices(
  x: ArrayLike<number>,
  y: ArrayLike<number>,
  threshold: number,
): number[] {
  const n = x.length;
  if (threshold >= n || threshold < 3)
    return Array.from({ length: n }, (_, i) => i);

  const val = (i: number) => {
    const v = y[i]!;
    return Number.isNaN(v) ? 0 : v;
  };
  const indices = [0];
  const bucketSize = (n - 2) / (threshold - 2);
  let a = 0;

  for (let bucket = 0; bucket < threshold - 2; bucket++) {
    const nextStart = Math.floor((bucket + 1) * bucketSize) + 1;
    const nextEnd = Math.min(Math.floor((bucket + 2) * bucketSize) + 1, n);
    let avgX = 0;
    let avgY = 0;
    for (let i = nextStart; i < nextEnd; i++) {
      avgX += x[i]!;
      avgY += val(i);
    }
    const count = Math.max(1, nextEnd - nextStart);
    avgX /= count;
    avgY /= count;

    const start = Math.floor(bucket * bucketSize) + 1;
    const end = Math.floor((bucket + 1) * bucketSize) + 1;
    let maxArea = -1;
    let chosen = start;
    for (let i = start; i < end; i++) {
      const area = Math.abs(
        (x[a]! - avgX) * (val(i) - val(a)) - (x[a]! - x[i]!) * (avgY - val(a)),
      );
      if (area > maxArea) {
        maxArea = area;
        chosen = i;
      }
    }
    indices.push(chosen);
    a = chosen;
  }
  indices.push(n - 1);
  return indices;
}

/** Picks elements of `values` at `indices` into a new typed array. */
export function pick(
  values: ArrayLike<number>,
  indices: readonly number[],
): Float64Array {
  const out = new Float64Array(indices.length);
  for (let i = 0; i < indices.length; i++) out[i] = values[indices[i]!]!;
  return out;
}
