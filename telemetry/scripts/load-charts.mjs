#!/usr/bin/env node
/**
 * Local chart load test: the mock Core streaming the given points/s into the Telemetry server, auth off.
 * Needs a build (`pnpm load:charts` builds first). Ctrl+C stops both processes.
 *
 *   pnpm load:charts            # 900 000 points/s
 *   pnpm load:charts 100000
 */
import { spawn } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const rate = Number(process.argv[2] ?? 900_000);
if (!Number.isFinite(rate) || rate <= 0) {
  console.error(`usage: pnpm load:charts [points per second], got ${process.argv[2]}`);
  process.exit(2);
}
const port = process.env.PORT || '8080';
const corePort = process.env.MOCK_CORE_PORT || '9470';
const cwd = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const children = [];
let stopping = false;

function start(label, script, env) {
  const child = spawn(process.execPath, [script], { cwd, env: { ...process.env, ...env } });
  const prefix = (line) => `${label.padEnd(9)}| ${line}`;
  for (const [from, to] of [
    [child.stdout, process.stdout],
    [child.stderr, process.stderr],
  ]) {
    let rest = '';
    from.on('data', (chunk) => {
      const lines = (rest + chunk).split('\n');
      rest = lines.pop();
      to.write(lines.map((line) => `${prefix(line)}\n`).join(''));
    });
  }
  child.on('exit', (code, signal) => {
    if (!stopping) {
      console.error(prefix(`exited (${signal ?? code}); stopping the other process`));
      stop(1);
    }
  });
  children.push(child);
}

function stop(code) {
  stopping = true;
  for (const child of children) {
    child.kill('SIGTERM');
  }
  process.exitCode = code;
}

process.on('SIGINT', () => stop(0));
process.on('SIGTERM', () => stop(0));

start('mock-core', 'scripts/mock-core.mjs', { MOCK_CORE_PORT: corePort, MOCK_CORE_RATE: String(rate) });
start('telemetry', 'dist/server/index.js', {
  PORT: port,
  CORE_TELEMETRY_URL: `http://127.0.0.1:${corePort}`,
  TELEMETRY_AUTH_DISABLED: '1',
});

console.log(
  `\n  ${rate.toLocaleString('en-GB')} pts/s → http://127.0.0.1:${port}\n` +
    '  UI tabs: LIVE / CHARTS / ANALYSIS. Ctrl+C stops.\n',
);
