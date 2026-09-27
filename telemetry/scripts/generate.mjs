import { spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const telemetryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(telemetryRoot, "..");
const protoFile = path.join(repoRoot, "core", "proto", "telemetry_v1.proto");
const protoInclude = path.join(repoRoot, "core", "proto");
const outDir = path.join(telemetryRoot, "src", "gen");
mkdirSync(outDir, { recursive: true });

const isWin = process.platform === "win32";
if (isWin) {
  console.warn("WARN: You are running this script on Windows - might not work. Try via WSL if it fails.");
}
const protocBin = path.join(telemetryRoot, "node_modules", "@protobuf-ts", "protoc", "protoc.js");
const tsProtoPlugin = path.join(
  telemetryRoot,
  "node_modules",
  ".bin",
  isWin ? "protoc-gen-ts_proto.cmd" : "protoc-gen-ts_proto",
);

const args = [
  protocBin,
  `-I${protoInclude}`,
  `--plugin=protoc-gen-ts_proto=${tsProtoPlugin}`,
  `--ts_proto_out=${outDir}`,
  "--ts_proto_opt=esModuleInterop=true,outputServices=nice-grpc,outputServices=generic-definitions,useExactTypes=false,forceLong=bigint",
  protoFile,
];

const result = spawnSync(process.execPath, args, { stdio: "inherit", cwd: telemetryRoot });

if (result.error) {
  console.error(result.error);
  process.exit(1);
}
if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
