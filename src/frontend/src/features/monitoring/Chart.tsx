import { t } from '../../shared/i18n.js';

export type Sample = { time: number; value: number };
const timeLabel = (ms: number) => new Date(ms).toISOString().replace('T', ' ').slice(0, 19);

/** A point is a measurement. Gaps between measurements have no implied values. */
export function TimeChart({ title, unit, points, range }: {
  title: string; unit: string; points: Sample[]; range?: readonly [number, number];
}) {
  const low = range?.[0] ?? points[0]?.time ?? 0;
  const high = range?.[1] ?? points.at(-1)?.time ?? low;
  const max = points.reduce((n, point) => Math.max(n, point.value), 0);
  const min = points.reduce((n, point) => Math.min(n, point.value), 0);
  return <figure className="monitor-chart">
    <figcaption className="fw-semibold">{title} · <bdi>{unit}</bdi></figcaption>
    {points.length === 0 ? <p className="text-secondary">{t("No measurements in this interval.")}</p> : <>
      <div className="monitor-scale" dir="ltr"><span>{max.toLocaleString(undefined, {maximumFractionDigits: 3})}</span><span>{min}</span>
        <svg role="img" aria-label={title} viewBox="0 0 600 106" preserveAspectRatio="none">
          <path d="M4 4 H596 M4 53 H596 M4 102 H596" fill="none" stroke="currentColor" opacity=".2" />
          {points.map((p, i) => <circle key={i} cx={high === low ? 300 : 4 + 592 * (p.time - low) / (high - low)}
            cy={102 - 98 * ((p.value - min) / (max - min || 1))} r="2.8" fill="currentColor">
            <title>{timeLabel(p.time)} UTC · {p.value} {unit}</title>
          </circle>)}
        </svg>
      </div>
      <div className="d-flex justify-content-between flex-wrap small" dir="ltr">
        <time>{timeLabel(low)} UTC</time>{low !== high && <time>{timeLabel(high)} UTC</time>}
      </div>
      <details><summary>{t("Measurements ({0})", [points.length])}</summary>
        <div className="monitor-values" tabIndex={0} role="region" aria-label={title}>
          <table className="table table-sm"><thead><tr><th scope="col">{t("Time (UTC)")}</th><th scope="col">{unit}</th></tr></thead>
            <tbody>{points.map((p, i) => <tr key={i}><td><bdi>{timeLabel(p.time)}</bdi></td><td>{p.value}</td></tr>)}</tbody></table>
        </div>
      </details>
    </>}
  </figure>;
}

export function BarChart({ title, rows, unit }: {
  title: string; rows: { label: string; value: number }[]; unit: string;
}) {
  const max = rows.reduce((n, row) => Math.max(n, row.value), 0);
  return <figure className="monitor-chart"><figcaption className="fw-semibold">{title} · {unit}</figcaption>
    {rows.length ? <ul className="list-unstyled mb-0">{rows.map(row => <li className="monitor-bar" key={row.label}>
      <bdi>{row.label}</bdi><span className="monitor-track" aria-hidden="true"><span style={{width: (100 * row.value / (max || 1)) + '%'}} /></span>
      <bdi>{row.value}</bdi></li>)}</ul> : <p>{t("No measurements in this interval.")}</p>}
  </figure>;
}
