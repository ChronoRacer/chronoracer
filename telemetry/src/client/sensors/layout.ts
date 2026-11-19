export type ZoneId = 'driving' | 'engine' | 'map' | 'vehicle' | 'service' | 'hidden';

export type Zone = { id: ZoneId; title: string; sensors: readonly string[] };

/**
 * Dashboard zones in screen order. Each contract sensor belongs to exactly one zone. `hidden` sensors are received
 * but not shown: GPS quality and duplicates of what the map and the speed gauge already say.
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
    title: 'NOT SHOWN',
    sensors: ['gps_sats', 'gps_pdop', 'gps_speed', 'gps_alt', 'baro'],
  },
];

export const STATUS_LABELS: Readonly<Record<string, Readonly<Record<number, string>>>> = {
  gps_fix: { 0: 'NO FIX', 2: '2D', 3: '3D' },
};

/** Live sensors that no zone places; shown as generic cards on the CHARTS tab. */
export function unknownSensors(liveIds: readonly string[]): readonly string[] {
  const placed = new Set(ZONES.flatMap((zone) => zone.sensors));
  return liveIds.filter((id) => !placed.has(id)).sort();
}
