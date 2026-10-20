import { useEffect, useRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { WINDOW_SEC, type SeriesStore } from '../lib/series-store';

export type ChartSeries = { id: string; color: string; dash?: number[] };

type Props = {
  store: SeriesStore;
  series: readonly ChartSeries[];
  min: number | null;
  max: number | null;
  height?: number;
};

const AXIS: uPlot.Axis = {
  stroke: '#6b7486',
  grid: { stroke: '#1a2130', width: 1 },
  ticks: { stroke: '#1a2130', width: 1 },
  font: '10px ui-monospace, monospace',
};
const clockTime = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});
// One line per tick: uPlot's default adds a date row that does not fit a small card.
const X_AXIS: uPlot.Axis = { ...AXIS, space: 70, values: (_plot, ticks) => ticks.map((t) => clockTime.format(t * 1000)) };

// The catalog range, widened when the data leaves it; without a range, the data's own.
const yRange =
  (min: number | null, max: number | null): uPlot.Range.Function =>
  (_plot, lo, hi) => {
    const hasData = Number.isFinite(lo) && Number.isFinite(hi);
    if (min === null || max === null) {
      return hasData ? uPlot.rangeNum(lo, hi, 0.1, true) : [0, 1];
    }
    return hasData ? [Math.min(min, lo), Math.max(max, hi)] : [min, max];
  };

// Created once per card; each store frame updates the data and slides the shared time window.
export function SensorChart({ store, series, min, max, height = 96 }: Props) {
  const rootRef = useRef<HTMLDivElement>(null);
  const key = series.map((s) => `${s.id}:${s.color}:${s.dash?.join(',') ?? ''}`).join('|');

  useEffect(() => {
    const el = rootRef.current;
    if (!el) {
      return;
    }
    const plot = new uPlot(
      {
        width: el.clientWidth || 300,
        height,
        legend: { show: false },
        cursor: { sync: { key: 'sensors' } },
        scales: { x: { time: true }, y: { range: yRange(min, max) } },
        series: [{}, ...series.map((s) => ({ stroke: s.color, width: 1.5, dash: s.dash }))],
        axes: [X_AXIS, { ...AXIS, size: 40, space: 18 }],
      },
      [[], ...series.map(() => [])],
      el,
    );

    let drawnVersion = '';
    const draw = () => {
      const version = series.map((s) => store.version(s.id)).join(',');
      plot.batch(() => {
        if (version !== drawnVersion) {
          drawnVersion = version;
          plot.setData(alignedData(store, series), false);
        }
        plot.setScale('x', { min: store.latest - WINDOW_SEC, max: store.latest });
      });
    };
    draw();
    const stopFrames = store.onFrame(draw);
    const resize = new ResizeObserver(() => plot.setSize({ width: el.clientWidth, height }));
    resize.observe(el);

    return () => {
      stopFrames();
      resize.disconnect();
      plot.destroy();
    };
    // `key` stands for `series`, which callers usually pass as a fresh literal.
  }, [store, key, min, max, height]);

  return <div className="chart" ref={rootRef} />;
}

// Series sampled at different instants share one x axis; uPlot spans the alignment holes but keeps NaN gaps.
function alignedData(store: SeriesStore, series: readonly ChartSeries[]): uPlot.AlignedData {
  const tables = series.map((s) => {
    const data = store.get(s.id);
    return [data?.times ?? [], data?.values ?? []] as uPlot.AlignedData;
  });
  return tables.length === 1 ? tables[0] : uPlot.join(tables);
}
