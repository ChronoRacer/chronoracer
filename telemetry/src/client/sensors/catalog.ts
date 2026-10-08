import contract from '../../../../infra/contract/sensors.json';

export type SensorSpec = {
  name: string;
  unit: string;
  min: number | null;
  max: number | null;
  description: string;
};

export const CAR: string = contract.car;

export const SENSORS: readonly SensorSpec[] = contract.sensors;

export const sensorSpec = (id: string): SensorSpec | undefined => SENSORS.find((s) => s.name === id);
