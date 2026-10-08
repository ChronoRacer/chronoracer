export type Section = { title: string; sensors: readonly string[] };

export const SECTIONS: readonly Section[] = [
  { title: 'Driving', sensors: ['steer_angle', 'steer_torque', 'accel_pedal', 'brake_prs', 'brake_prs_b'] },
  { title: 'Speed and wheels', sensors: ['speed', 'gps_speed', 'wheel_fl', 'wheel_fr', 'wheel_rl', 'wheel_rr'] },
  { title: 'Dynamics', sensors: ['accel_long', 'accel_lat'] },
  { title: 'Engine', sensors: ['rpm', 'throttle', 'engine_load', 'maf', 'ign_timing'] },
  { title: 'Mixture', sensors: ['lambda_cmd', 'lambda', 'stft', 'ltft', 'o2_current'] },
  { title: 'Engine temperatures', sensors: ['coolant_temp', 'oil_temp', 'intake_temp'] },
  { title: 'Energy and fuel', sensors: ['hv_soc', 'batt_volt', 'fuel_l'] },
  { title: 'Ambient', sensors: ['ambient_temp', 'baro'] },
  { title: 'GPS', sensors: ['gps_fix', 'gps_sats', 'gps_pdop', 'gps_lat', 'gps_lon', 'gps_alt', 'gps_heading'] },
];

export const STATUS_LABELS: Readonly<Record<string, Readonly<Record<number, string>>>> = {
  gps_fix: { 0: 'no fix', 2: '2D', 3: '3D' },
};

export function withUnknownSensors(liveIds: readonly string[]): readonly Section[] {
  const placed = new Set(SECTIONS.flatMap((section) => section.sensors));
  const unknown = liveIds.filter((id) => !placed.has(id)).sort();
  return unknown.length === 0 ? SECTIONS : [...SECTIONS, { title: 'Other', sensors: unknown }];
}
