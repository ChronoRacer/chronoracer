export type ZoneId = 'status' | 'driving' | 'powertrain' | 'service';

export type Zone = { id: ZoneId; title: string; sensors: readonly string[] };

/** Dashboard zones in screen order. Each contract sensor belongs to exactly one zone. */
export const ZONES: readonly Zone[] = [
  {
    id: 'status',
    title: 'Status i GPS',
    sensors: ['gps_fix', 'gps_sats', 'gps_pdop', 'batt_volt', 'fuel_l', 'ambient_temp', 'baro', 'gps_lat', 'gps_lon', 'gps_alt'],
  },
  {
    id: 'driving',
    title: 'Jazda i dynamika',
    sensors: [
      'speed',
      'gps_speed',
      'gps_heading',
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
    id: 'powertrain',
    title: 'Hybryda i napęd',
    sensors: ['hv_soc', 'rpm', 'throttle', 'engine_load'],
  },
  {
    id: 'service',
    title: 'Serwis i telemetria silnika',
    sensors: [
      'coolant_temp',
      'oil_temp',
      'intake_temp',
      'lambda',
      'lambda_cmd',
      'stft',
      'ltft',
      'maf',
      'ign_timing',
      'o2_current',
    ],
  },
];

export const STATUS_LABELS: Readonly<Record<string, Readonly<Record<number, string>>>> = {
  gps_fix: { 0: 'brak fix', 2: '2D', 3: '3D' },
};

/** Live sensors that no zone places; shown as generic cards after the zones. */
export function unknownSensors(liveIds: readonly string[]): readonly string[] {
  const placed = new Set(ZONES.flatMap((zone) => zone.sensors));
  return liveIds.filter((id) => !placed.has(id)).sort();
}
