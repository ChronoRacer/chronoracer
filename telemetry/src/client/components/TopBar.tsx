import type { TelemetryState } from '../hooks/useTelemetrySocket';
import { CAR } from '../sensors/catalog';
import { ConnectionStatus } from './ConnectionStatus';
import { DriverPanel } from './DriverPanel';
import { GapMarkers } from './GapMarkers';
import { cx } from './instruments';

export type Tab = 'live' | 'charts' | 'analysis';

const TABS: { id: Tab; label: string }[] = [
  { id: 'live', label: 'LIVE' },
  { id: 'charts', label: 'CHARTS' },
  { id: 'analysis', label: 'ANALYSIS' },
];

type Props = {
  state: TelemetryState;
  tab: Tab;
  onTab: (tab: Tab) => void;
  /** Alerts that arrived since ANALYSIS was last open. */
  unseenAlerts: number;
  onDriver: () => void;
};

/** One row: brand, tabs, connection and gaps, driver radio, logout. */
export function TopBar({ state, tab, onTab, unseenAlerts, onDriver }: Props) {
  return (
    <header className="topbar">
      <div className="brand" title={CAR}>
        <img src="/chronoracer-logo.svg" alt="" className="brand-logo" />
        <div className="brand-text">
          <h1 className="brand-name">CHRONORACER</h1>
          <span className="brand-car">{CAR}</span>
        </div>
      </div>
      <nav className="tabs" aria-label="Views">
        {TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            className={cx('tab', tab === t.id && 'active')}
            aria-current={tab === t.id ? 'page' : undefined}
            onClick={() => onTab(t.id)}
          >
            {t.label}
            {t.id === 'analysis' && unseenAlerts > 0 ? (
              <span className="tab-badge" title={`${unseenAlerts} new alerts`}>
                {unseenAlerts}
              </span>
            ) : null}
          </button>
        ))}
      </nav>
      <ConnectionStatus connection={state.connection} coreConnected={state.coreConnected} hello={state.hello} />
      <GapMarkers gaps={state.gaps} />
      <DriverPanel
        enabled={state.driverEnabled}
        messages={state.driverMessages}
        active={state.driverActive}
        variant="strip"
        onExpand={onDriver}
      />
      <a className="logout" href="/auth/logout">
        LOG OUT
      </a>
    </header>
  );
}
