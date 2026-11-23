export type ZoneId = 'driving' | 'engine' | 'map' | 'vehicle' | 'service' | 'hidden';

export type Zone = { id: ZoneId; title: string; sensors: readonly string[] };

/**
 * Dashboard zones in screen order. Each contract sensor belongs to exactly one zone. `hidden` sensors have no LIVE
 * instrument (GPS quality and duplicates of what the map and the speed gauge already say); like every sensor without a
 * MAIN chart, they are charted on CHARTS → OTHER.
 */
export const ZONES: readonly Zone[] = [
  {
    id: 'driving',
    title: 'DRIVING',
    sensors: [
      'speed',
      'accel_pedal',
      'brake_prs',
      'brake_prs_b',
      'steer_angle',
      'steer_torque',
      'wheel_fl',
      'wheel_fr',
      'wheel_rl',
      'wheel_rr',
      'accel_long',
      'accel_lat',
    ],
  },
  {
    id: 'engine',
    title: 'ENGINE',
    sensors: ['rpm', 'throttle', 'engine_load'],
  },
  {
    id: 'map',
    title: 'TRACK MAP',
    sensors: ['gps_lat', 'gps_lon', 'gps_heading', 'gps_fix'],
  },
  {
    id: 'vehicle',
    title: 'ENERGY / TEMPERATURES',
    sensors: ['hv_soc', 'fuel_l', 'batt_volt', 'coolant_temp', 'oil_temp', 'intake_temp', 'ambient_temp'],
  },
  {
    id: 'service',
    title: 'SERVICE CHARTS',
    sensors: ['lambda', 'lambda_cmd', 'stft', 'ltft', 'maf', 'ign_timing', 'o2_current'],
  },
  {
    id: 'hidden',
    title: 'NOT ON LIVE',
    sensors: ['gps_sats', 'gps_pdop', 'gps_speed', 'gps_alt', 'baro'],
  },
];

export const STATUS_LABELS: Readonly<Record<string, Readonly<Record<number, string>>>> = {
  gps_fix: { 0: 'NO FIX', 2: '2D', 3: '3D' },
};

/** Sensors charted on CHARTS → MAIN (`ServiceZone`), in no particular order. */
export const MAIN_CHARTS: readonly string[] = [
  'coolant_temp',
  'oil_temp',
  'intake_temp',
  'maf',
  'ign_timing',
  'lambda',
  'lambda_cmd',
  'stft',
  'ltft',
  'o2_current',
];

export type ChartGroup = { title: string; sensors: readonly string[] };

/** Live sensors that no zone places; charted on CHARTS → OTHER. */
export function unknownSensors(liveIds: readonly string[]): readonly string[] {
  const placed = new Set(ZONES.flatMap((zone) => zone.sensors));
  return liveIds.filter((id) => !placed.has(id)).sort();
}

/** CHARTS → OTHER: per zone, every sensor without a MAIN chart, then live sensors missing from the contract. */
export function otherChartGroups(liveIds: readonly string[]): readonly ChartGroup[] {
  const main = new Set(MAIN_CHARTS);
  const groups: ChartGroup[] = ZONES.map((zone) => ({
    title: zone.title,
    sensors: zone.sensors.filter((id) => !main.has(id)),
  })).filter((group) => group.sensors.length > 0);
  const unknown = unknownSensors(liveIds);
  return unknown.length > 0 ? [...groups, { title: 'NOT IN CONTRACT', sensors: unknown }] : groups;
}
