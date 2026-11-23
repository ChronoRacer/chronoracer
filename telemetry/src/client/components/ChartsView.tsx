import type { SeriesStore } from '../lib/series-store';
import { MAIN_CHARTS, otherChartGroups } from '../sensors/layout';
import { cx } from './instruments';
import { SensorCard, ServiceZone } from './ServiceZone';

export type ChartsSet = 'main' | 'other';

/** OTHER cards per page: two rows of five fill the stage like MAIN, so nothing scrolls. */
export const OTHER_PAGE_SIZE = 10;

type Props = {
  store: SeriesStore;
  /** Sensor ids received so far, for the ones missing from the contract. */
  sensorIds: readonly string[];
  set: ChartsSet;
  onSet: (set: ChartsSet) => void;
  /** OTHER page, zero-based; clamped when the sensor list shrinks. */
  page: number;
  onPage: (page: number) => void;
};

const SET_HINT: Record<ChartsSet, string> = {
  main: 'temperatures, mixture, trims, MAF, ignition',
  other: 'every other sensor, in LIVE zone order',
};

/** CHARTS tab: MAIN holds the service charts; OTHER pages through every remaining sensor, so none is out of sight. */
export function ChartsView({ store, sensorIds, set, onSet, page, onPage }: Props) {
  const cards = otherChartGroups(sensorIds).flatMap((group) => group.sensors.map((id) => ({ id, group: group.title })));
  const pages = Math.max(1, Math.ceil(cards.length / OTHER_PAGE_SIZE));
  const current = Math.min(page, pages - 1);
  const first = current * OTHER_PAGE_SIZE;
  const shown = cards.slice(first, first + OTHER_PAGE_SIZE);
  const counts: Record<ChartsSet, number> = { main: MAIN_CHARTS.length, other: cards.length };

  return (
    <div className="charts-view">
      <nav className="subtabs" aria-label="Chart sets">
        {(['main', 'other'] as const).map((id) => (
          <button
            key={id}
            type="button"
            className={cx('subtab', set === id && 'active')}
            aria-current={set === id ? 'page' : undefined}
            onClick={() => onSet(id)}
          >
            {id.toUpperCase()}
            <span className="subtab-count">{counts[id]}</span>
          </button>
        ))}
        <span className="hint">{SET_HINT[set]}</span>
        {set === 'other' && pages > 1 ? (
          <Pager current={current} pages={pages} first={first} shown={shown.length} total={cards.length} onPage={onPage} />
        ) : null}
      </nav>
      {set === 'main' ? (
        <ServiceZone store={store} />
      ) : (
        <section className="charts charts-other" aria-label={`Other sensor charts, page ${current + 1} of ${pages}`}>
          {shown.map(({ id, group }) => (
            <SensorCard key={id} store={store} id={id} note={group} />
          ))}
        </section>
      )}
    </div>
  );
}

function Pager({
  current,
  pages,
  first,
  shown,
  total,
  onPage,
}: {
  current: number;
  pages: number;
  first: number;
  shown: number;
  total: number;
  onPage: (page: number) => void;
}) {
  return (
    <div className="pager" role="group" aria-label="Pages">
      <span className="subtab-count">
        {first + 1}–{first + shown} OF {total}
      </span>
      <button
        type="button"
        className="subtab"
        onClick={() => onPage(current - 1)}
        disabled={current === 0}
        aria-label="Previous page"
      >
        ‹
      </button>
      {Array.from({ length: pages }, (_, i) => (
        <button
          key={i}
          type="button"
          className={cx('subtab', i === current && 'active')}
          aria-current={i === current ? 'page' : undefined}
          onClick={() => onPage(i)}
        >
          {i + 1}
        </button>
      ))}
      <button
        type="button"
        className="subtab"
        onClick={() => onPage(current + 1)}
        disabled={current === pages - 1}
        aria-label="Next page"
      >
        ›
      </button>
    </div>
  );
}
