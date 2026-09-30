import { scaleLinear, scaleTime } from "d3-scale";
import { area, line } from "d3-shape";
import { timeFormat } from "d3-time-format";
import {
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { valuesAt } from "./values-at";

export interface ChartSeries {
  label: string;
  color: string;
  values: Float64Array;
  fill?: boolean;
}

interface TimeChartProps {
  title: ReactNode;
  t: Float64Array;
  series: ChartSeries[];
  /** When set, the visible range slides with the clock; otherwise it spans the data. */
  follow?: { windowMs: number; lagMs: number };
  format: (v: number) => string;
  hoverMs: number | null;
  onHover: (ms: number | null) => void;
  onWidth?: (width: number) => void;
  height?: number;
}

const MARGIN = { top: 12, right: 12, bottom: 22, left: 52 };
const GRID = "rgba(148, 163, 184, 0.12)";
const AXIS = "#94a3b8";
const shortTime = timeFormat("%H:%M:%S");
const longTime = timeFormat("%b %d %H:%M");

/**
 * Canvas time-series chart. SVG creates a DOM node per path segment; canvas draws the
 * already-downsampled columns directly, so redrawing at display refresh rate is cheap.
 */
export function TimeChart(props: TimeChartProps) {
  const { title, format, hoverMs, onHover, onWidth, height = 200 } = props;
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [width, setWidth] = useState(0);
  const propsRef = useRef(props);

  useLayoutEffect(() => {
    propsRef.current = props;
  });

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => {
      const w = Math.floor(entry!.contentRect.width);
      setWidth(w);
      onWidth?.(w - MARGIN.left - MARGIN.right);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, [onWidth]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || width === 0) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    const ctx = canvas.getContext("2d")!;
    let raf = 0;
    const render = () => {
      draw(ctx, propsRef.current, width, height, dpr);
      if (propsRef.current.follow) raf = requestAnimationFrame(render);
    };
    render();
    return () => cancelAnimationFrame(raf);
    // Re-run when data or hover change so static (non-follow) charts redraw.
  }, [width, height, props.t, props.series, props.follow, hoverMs]);

  const onMove = (e: React.PointerEvent<HTMLCanvasElement>) => {
    const x = e.clientX - e.currentTarget.getBoundingClientRect().left;
    const [from, to] = currentDomain(propsRef.current);
    const scale = scaleTime()
      .domain([from, to])
      .range([MARGIN.left, width - MARGIN.right]);
    onHover(
      x < MARGIN.left || x > width - MARGIN.right
        ? null
        : scale.invert(x).getTime(),
    );
  };

  const readout =
    hoverMs !== null ? valuesAt(props.t, props.series, hoverMs) : null;
  return (
    <section className="panel chart">
      <header className="panel-header">
        <h2>{title}</h2>
        <ul className="legend">
          {props.series.map((s, i) => (
            <li key={s.label}>
              <span className="swatch" style={{ background: s.color }} />
              {s.label}
              {readout && <strong>{format(readout.values[i]!)}</strong>}
            </li>
          ))}
          {readout && (
            <li className="muted">{shortTime(new Date(readout.t))}</li>
          )}
        </ul>
      </header>
      <div ref={wrapRef} className="chart-body" style={{ height }}>
        <canvas
          ref={canvasRef}
          style={{ width, height }}
          onPointerMove={onMove}
          onPointerLeave={() => onHover(null)}
          role="img"
          aria-label={typeof title === "string" ? `${title} chart` : "chart"}
        />
      </div>
    </section>
  );
}

function currentDomain(p: TimeChartProps): [number, number] {
  if (p.follow) {
    const end = Date.now() - p.follow.lagMs;
    return [end - p.follow.windowMs, end];
  }
  return p.t.length > 1 ? [p.t[0]!, p.t[p.t.length - 1]!] : [0, 1];
}

function draw(
  ctx: CanvasRenderingContext2D,
  p: TimeChartProps,
  width: number,
  height: number,
  dpr: number,
) {
  const [from, to] = currentDomain(p);
  const x = scaleTime()
    .domain([from, to])
    .range([MARGIN.left, width - MARGIN.right]);
  let maxY = 0;
  for (const s of p.series) {
    for (let i = 0; i < s.values.length; i++) {
      const v = s.values[i]!;
      if (v > maxY && p.t[i]! >= from) maxY = v;
    }
  }
  const y = scaleLinear()
    .domain([0, maxY > 0 ? maxY * 1.1 : 1])
    .nice(4)
    .range([height - MARGIN.bottom, MARGIN.top]);

  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.font = "11px system-ui, sans-serif";
  ctx.fillStyle = AXIS;
  ctx.strokeStyle = GRID;
  ctx.lineWidth = 1;

  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (const tick of y.ticks(4)) {
    const py = Math.round(y(tick)) + 0.5;
    ctx.beginPath();
    ctx.moveTo(MARGIN.left, py);
    ctx.lineTo(width - MARGIN.right, py);
    ctx.stroke();
    ctx.fillText(p.format(tick), MARGIN.left - 6, py);
  }
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  const fmt = to - from > 24 * 3600_000 ? longTime : shortTime;
  for (const tick of x.ticks(Math.max(2, Math.floor(width / 110)))) {
    ctx.fillText(fmt(tick), x(tick), height - MARGIN.bottom + 6);
  }

  ctx.save();
  ctx.beginPath();
  ctx.rect(
    MARGIN.left,
    MARGIN.top,
    width - MARGIN.left - MARGIN.right,
    height - MARGIN.top - MARGIN.bottom,
  );
  ctx.clip();
  const idx = Array.from({ length: p.t.length }, (_, i) => i);
  for (const s of p.series) {
    const defined = (i: number) => !Number.isNaN(s.values[i]!);
    if (s.fill) {
      ctx.beginPath();
      area<number>()
        .defined(defined)
        .x((i) => x(p.t[i]!))
        .y0(y(0))
        .y1((i) => y(s.values[i]!))
        .context(ctx)(idx);
      ctx.fillStyle = `${s.color}33`;
      ctx.fill();
    }
    ctx.beginPath();
    line<number>()
      .defined(defined)
      .x((i) => x(p.t[i]!))
      .y((i) => y(s.values[i]!))
      .context(ctx)(idx);
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 1.5;
    ctx.stroke();
  }
  if (p.hoverMs !== null && p.hoverMs >= from && p.hoverMs <= to) {
    const hx = Math.round(x(p.hoverMs)) + 0.5;
    ctx.strokeStyle = "rgba(226, 232, 240, 0.5)";
    ctx.beginPath();
    ctx.moveTo(hx, MARGIN.top);
    ctx.lineTo(hx, height - MARGIN.bottom);
    ctx.stroke();
  }
  ctx.restore();
}
