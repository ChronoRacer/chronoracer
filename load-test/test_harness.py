"""Offline regression checks: python -m unittest discover -s load-test -p 'test_*.py'."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from collect_metrics import HEADERS, CORE_METRICS, fetch, parse_rfc3339
from export_flow import export_checks
from steady_summary import counter_intervals, distribution_summary, summarize
from summarize_generators import generator_summary


def timestamp(seconds):
    from datetime import datetime, timedelta, timezone
    return (datetime(2026, 10, 24, tzinfo=timezone.utc) + timedelta(seconds=seconds)).isoformat()


def row(begin, end, value, labels=None):
    return {"start_time": timestamp(begin), "end_time": timestamp(end),
            "resource_type": "gce_instance", "resource_labels": '{"instance_id":"123"}',
            "metric_labels": json.dumps(labels or {}, sort_keys=True), "value": str(value)}


class HarnessTests(unittest.TestCase):
    def test_counter_reset_uses_epoch_even_when_value_increases(self):
        rows = [row(0, 60, 100), row(0, 120, 200), row(130, 180, 500), row(130, 240, 600)]
        result = counter_intervals(list(reversed(rows)))
        self.assertEqual([r['delta'] for r in result], [100, 100, 500, 100])
        self.assertEqual(result[2]['seconds'], 50)

    def test_counter_outcomes_stay_separate(self):
        rows = [row(0, 60, 100, {'outcome': 'ok'}), row(0, 60, 2, {'outcome': 'retry'}),
                row(0, 120, 130, {'outcome': 'ok'}), row(0, 120, 5, {'outcome': 'retry'})]
        result = counter_intervals(rows)
        self.assertEqual({r['labels']['outcome']: r['delta'] for r in result if r['end'] == parse_rfc3339(timestamp(120))},
                         {'ok': 30, 'retry': 3})

    def test_distribution_merge_is_weighted_and_preserves_overflow(self):
        def distribution(count, mean, counts):
            return json.dumps({'count': str(count), 'mean': mean,
                'bucket_options': {'explicit_buckets': {'bounds': [10, 100]}},
                'bucket_counts': [str(v) for v in counts]})
        rows = [row(60, 120, distribution(100, 10, [90, 10, 0])),
                row(120, 180, distribution(2, 500, [0, 0, 2])),
                row(0, 60, distribution(1000, 9000, [0, 0, 1000]))]
        result = distribution_summary(rows, parse_rfc3339(timestamp(60)), parse_rfc3339(timestamp(180)))
        self.assertEqual(result['batch_samples'], 102)
        self.assertAlmostEqual(result['mean_ms'], 2000 / 102)
        self.assertEqual(result['p99'], {'lower_bound_ms': 100, 'overflow': True})
        self.assertIsNone(distribution_summary([], parse_rfc3339(timestamp(0)), parse_rfc3339(timestamp(180))))

    def test_generator_final_totals_not_double_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'gen-0.log'
            path.write_text('[Report] window_s=61.0 sent_pts_s=100000 sent_msg_s=9000 behind_max_s=0.1 '
                            'queued=3 delivered=1 failed=2 delivery_ms=p99>1000\n'
                            '[Report] window_s=10.0 sent_pts_s=100000 sent_msg_s=9000 behind_max_s=0.1 '
                            'queued=0 delivered=3 failed=1 delivery_ms=p99<=100\n'
                            '[Sensors] Sent 4 messages, 42 points in 71.0s (1 points/s, target 100000).\n'
                            '[Summary] window_s=71.0 sent_pts_s=100000 sent_msg_s=9000 behind_max_s=0.1 '
                            'queued=0 delivered=4 failed=3 delivery_ms=p99>1000\n')
            result = generator_summary([path])
            self.assertEqual(result['total_failed'], 3)
            self.assertEqual(result['total_points'], 42)
            self.assertEqual(result['steady_total_pts_s'], 100000)
            path.write_text('[Report] malformed partial output')
            self.assertFalse(generator_summary([path])['complete'])
            self.assertIsNone(generator_summary([path])['total_points'])

    def test_monitoring_preserves_labels_epoch_and_distribution(self):
        from google.cloud import monitoring_v3
        client = Mock()
        series = monitoring_v3.TimeSeries({
            'metric': {'type': 'custom.googleapis.com/chronoracer/pipeline_persist_latency_ms'},
            'resource': {'type': 'gce_instance', 'labels': {'instance_id': '123', 'zone': 'europe-west1-b'}},
            'metric_kind': 'DELTA', 'value_type': 'DISTRIBUTION',
            'points': [{'interval': {'start_time': parse_rfc3339(timestamp(60)), 'end_time': parse_rfc3339(timestamp(120))},
                        'value': {'distribution_value': {'count': 2, 'mean': 50,
                            'bucket_options': {'explicit_buckets': {'bounds': [10, 100]}}, 'bucket_counts': [0, 2, 0]}}}]})
        client.list_time_series.return_value = [series]
        result = fetch(client, 'project', series.metric.type, 'gce_instance', {'instance_id': '123'},
                       parse_rfc3339(timestamp(0)), parse_rfc3339(timestamp(180)))
        parsed = dict(zip(HEADERS, result[0]))
        self.assertEqual(parsed['metric_kind'], 'DELTA')
        self.assertEqual(json.loads(parsed['resource_labels'])['instance_id'], '123')
        self.assertEqual(json.loads(parsed['value'])['bucket_counts'], ['0', '2', '0'])
        self.assertIn('resource.labels.instance_id="123"', client.list_time_series.call_args.kwargs['request']['filter'])
        self.assertNotIn('aggregation', client.list_time_series.call_args.kwargs['request'])

    def test_missing_metrics_do_not_become_zero_or_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp)
            (run / 'run-metadata.json').write_text(json.dumps({
                'start_utc': timestamp(120), 'end_utc': timestamp(720), 'instances': 4}))
            result = summarize(run, 60)
            self.assertIsNone(result['accepted_lines_check']['matches'])
            self.assertIsNone(result['accepted_lines_check']['observed'])
            self.assertEqual(set(result['missing_steady_metrics']), set(CORE_METRICS))
            self.assertFalse(result['generators']['complete'])
            self.assertIsNone(result['core_cpu_pct'])

    def test_export_notification_cannot_mask_failed_kafka_export(self):
        config = {'bucket': 'bucket', 'db_path': 'export/db'}
        log = ('Database export finished. Exported 1 out of 1 tables. Time taken: 1s\n'
               'Database export complete with 1 files\n'
               'Uploaded file: race/export/db/sensor.parquet\n'
               'Published shutdown message, id=123\nShutdown sequence complete\n')
        objects = ['gs://bucket/race/export/db/sensor.parquet']
        result = export_checks(log, objects, config, 'race')
        self.assertTrue(result['pubsub_published'])
        self.assertFalse(result['kafka_export_complete'])
        self.assertTrue(result['database_objects_complete'])
        self.assertTrue(all(export_checks(log + 'Kafka export complete', objects, config, 'race').values()))
        self.assertFalse(export_checks(log, [], config, 'race')['database_objects_complete'])
        self.assertFalse(export_checks(log.replace('with 1 files', 'with 2 files'), objects, config, 'race')['database_objects_complete'])

    def test_generator_orchestration_rate_split_and_delivery_failure(self):
        script = Path(__file__).resolve().parent / 'run-generators.sh'
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            fake = base / 'fake-python'
            fake.write_text('#!' + sys.executable + "\n" + r"""
import os, sys
if len(sys.argv) > 2 and sys.argv[1] == '-u':
    args = sys.argv[3:]
    rate = args[args.index('--rate') + 1]
    failed = os.environ.get('FAKE_FAILED', '0')
    print(f'[Sensors] Sent 10 messages, {rate} points in 1.0s (100 points/s, target {rate}).')
    print(f'[Summary] window_s=1.0 sent_pts_s={rate} sent_msg_s=10 behind_max_s=0.0 queued=0 delivered=10 failed={failed} delivery_ms=p99<=10')
else:
    os.execv(sys.executable, [sys.executable] + sys.argv[1:])
""")
            fake.chmod(0o755)
            env = dict(os.environ, PYTHON=str(fake), ALLOW_UNSYNCED='1', OKTA_CLIENT_SECRET='test-only', RACE_ID='unit-test')
            out = base / "run with 'quotes' and $signs"
            result = subprocess.run(['bash', str(script), '100003', '1', '4', str(out)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            summary = json.loads((out / 'generator-summary.json').read_text())
            self.assertEqual(summary['total_points'], 100003)
            self.assertEqual([i['total_points'] for i in summary['instances']], [25001, 25001, 25001, 25000])
            self.assertEqual(json.loads((out / 'run-metadata.json').read_text())['generator_exit'], 0)
            env['FAKE_FAILED'] = '1'
            failed = base / 'delivery-failed'
            result = subprocess.run(['bash', str(script), '100000', '1', '4', str(failed)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
            self.assertEqual(json.loads((failed / 'run-metadata.json').read_text())['generator_exit'], 1)


if __name__ == '__main__':
    unittest.main()
