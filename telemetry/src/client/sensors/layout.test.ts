import { describe, expect, it } from 'vitest';
import { SENSORS } from './catalog';
import { MAIN_CHARTS, STATUS_LABELS, ZONES, otherChartGroups, unknownSensors } from './layout';

describe('sensor contract', () => {
  it('has unique Kafka-valid ids and ordered ranges', () => {
    const names = SENSORS.map((s) => s.name);
    expect(new Set(names).size).toBe(names.length);
    for (const s of SENSORS) {
      expect(s.name).toMatch(/^[!-~]{1,12}$/);
      expect(s.min === null || s.max === null || s.min < s.max).toBe(true);
    }
  });
});

describe('dashboard layout', () => {
  it('places every catalog sensor in exactly one zone', () => {
    const placed = ZONES.flatMap((z) => z.sensors);
    expect([...placed].sort()).toEqual(SENSORS.map((s) => s.name).sort());
  });

  it('labels only catalog sensors as status', () => {
    for (const id of Object.keys(STATUS_LABELS)) {
      expect(SENSORS.some((s) => s.name === id)).toBe(true);
    }
  });

  it('charts every catalog sensor exactly once across MAIN and OTHER', () => {
    for (const id of MAIN_CHARTS) {
      expect(SENSORS.some((s) => s.name === id)).toBe(true);
    }
    const other = otherChartGroups([]).flatMap((g) => g.sensors);
    expect([...MAIN_CHARTS, ...other].sort()).toEqual(SENSORS.map((s) => s.name).sort());
  });

  it('adds live sensors missing from the catalog as the last OTHER group', () => {
    expect(otherChartGroups(['rpm', 'zz']).at(-1)).toEqual({ title: 'NOT IN CONTRACT', sensors: ['zz'] });
    expect(otherChartGroups(['rpm']).some((g) => g.title === 'NOT IN CONTRACT')).toBe(false);
  });

  it('lists live sensors missing from the catalog, sorted', () => {
    expect(unknownSensors(['rpm', 'zz', 'aa'])).toEqual(['aa', 'zz']);
    expect(unknownSensors(['rpm'])).toEqual([]);
  });
});
