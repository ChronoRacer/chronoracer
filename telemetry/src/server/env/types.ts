export type SensorSelection = string[] | undefined;

export type RingLimits = {
  maxPointsPerSensor: number;
  maxAgeMs: number;
  maxSensors: number;
  maxTotalPoints: number;
};

export type FanoutConfig = {
  tickMs: number;
  maxBufferedBytes: number;
};

export type AuthConfig =
  | { enabled: false }
  | {
      enabled: true;
      clientId: string;
      clientSecret: string;
      issuer: string;
      rootUrl: string;
      sessionSecret: string;
    };

export type TelemetryEnv = {
  port: number;
  coreTelemetryUrl: string;
  raceId: string | undefined;
  sensors: SensorSelection;
  ring: RingLimits;
  fanout: FanoutConfig;
  auth: AuthConfig;
  clientDir: string;
};
