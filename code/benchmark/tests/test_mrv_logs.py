import csv
import json
import os
import tempfile
import unittest

from benchmark.config import BenchParameters, DEFAULT_MRV_WINDOW, NodeParameters
from benchmark.aggregate import LEGACY_MRV_WINDOW, Setup
from benchmark.logs import (
    LogParser,
    ParseError,
    _classify_mrv_cross_replica,
    _parse_mrv_slice_line,
)
from benchmark.utils import PathMaker


def valid_slice_line(**overrides):
    fields = {
        'slice_id': 7,
        'mrv_window': 4,
        'slice_size': 4,
        'slice_max_round': 10,
        'seal_horizon': 14,
        'seal_wait_rounds': 4,
        'seal_delay_ms': 27,
        'release_delay_ms': 'unavailable',
        'snapshot_frontier': 14,
        'trigger_slice_id': 9,
        'trigger_export_prefix_digest': 'cHJlZml4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        'execution_order_digest': 'ZXhlY3V0aW9uAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        'eligible_vertex_count': 3,
        'ineligible_vertex_count': 1,
        'all_pair_count': 6,
        'causal_pair_count': 1,
        'incomparable_pair_count': 5,
        'eligible_pair_count': 3,
        'ineligible_pair_count': 2,
        'edge_count': 1,
        'conflict_count': 1,
        'no_signal_count': 1,
        'intra_scc_ordering_edge_count': 0,
        'inter_scc_ordering_edge_count': 1,
        'constrained_pair_count': 2,
        'scc_count': 4,
        'nontrivial_scc_count': 0,
        'vertices_in_nontrivial_sccs': 0,
        'max_scc_size': 1,
        'incomparable_pair_inversion_count': 1,
        'constrained_inversion_count': 1,
        'unconstrained_inversion_count': 0,
        'moved_vertex_count': 2,
        'unchanged_slice': 'false',
        'position_displacement_median': 0.333333,
        'position_displacement_p95': 0.666667,
    }
    fields.update(overrides)
    values = ' '.join(f'{key}={value}' for key, value in fields.items())
    return f'[benchmark] MRV_SliceStats {values}'


def registered_slice_line(**overrides):
    fields = {
        'slice_id': 7,
        'leader_round': 10,
        'leader_digest': 'bGVhZGVyAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        'slice_size': 4,
        'seal_horizon': 14,
        'member_set_digest': 'c2V0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        'member_order_digest': 'b3JkZXIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        'cumulative_member_count': 20,
        'export_prefix_digest': 'ZXhwb3J0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
    }
    fields.update(overrides)
    values = ' '.join(f'{key}={value}' for key, value in fields.items())
    return f'[benchmark] MRV_SliceRegistered {values}'


def valid_primary_config_log():
    return '\n'.join(
        (
            'Header size set to 1000 B',
            'Max header delay set to 200 ms',
            'Garbage collection depth set to 50 rounds',
            'MRV window set to 4 rounds',
            'Sync retry delay set to 10000 ms',
            'Sync retry nodes set to 3 nodes',
            'Batch size set to 500000 B',
            'Max batch delay set to 200 ms',
            'Primary booted on 127.0.0.1',
        )
    )


class MrvSliceParserTests(unittest.TestCase):
    def test_performance_setup_keeps_mrv_window_as_group_dimension(self):
        base = (
            'Faults: 0\nCommittee size: 4\nWorker(s) per node: 1\n'
            'Collocate primary and workers: True\nInput rate: 50000 tx/s\n'
            'Transaction size: 512 B\n'
        )
        setup_2 = Setup.from_str(f'{base}MRV window: 2 round(s)\n')
        setup_4 = Setup.from_str(f'{base}MRV window: 4 round(s)\n')
        setup_50 = Setup.from_str(f'{base}MRV window: 50 round(s)\n')
        legacy_setup = Setup.from_str(base)

        self.assertNotEqual(setup_2, setup_4)
        self.assertEqual(setup_2.mrv_window, 2)
        self.assertEqual(legacy_setup.mrv_window, LEGACY_MRV_WINDOW)
        self.assertNotEqual(legacy_setup, setup_50)

    def test_parses_counts_rates_and_unavailable_release_delay(self):
        record = _parse_mrv_slice_line(valid_slice_line())

        self.assertEqual(record['data_status'], 'valid')
        self.assertIsNone(record['release_delay_ms'])
        self.assertEqual(record['release_delay_status'], 'unavailable')
        self.assertAlmostEqual(record['direct_edge_coverage'], 1 / 5)
        self.assertAlmostEqual(record['strict_direct_edge_coverage'], 1 / 5)
        self.assertAlmostEqual(record['constrained_pair_coverage'], 2 / 5)
        self.assertEqual(record['unconstrained_pair_count'], 3)
        self.assertAlmostEqual(record['constrained_inversion_rate'], 1 / 2)
        self.assertAlmostEqual(record['unconstrained_inversion_rate'], 0 / 3)
        self.assertAlmostEqual(record['moved_vertex_rate'], 2 / 4)
        self.assertEqual(record['edge_yield_status'], 'valid')
        self.assertEqual(record['derived_status'], 'valid')

    def test_zero_denominator_is_blank_and_explicit(self):
        record = _parse_mrv_slice_line(
            valid_slice_line(
                slice_size=1,
                eligible_vertex_count=1,
                ineligible_vertex_count=0,
                all_pair_count=0,
                causal_pair_count=0,
                incomparable_pair_count=0,
                eligible_pair_count=0,
                ineligible_pair_count=0,
                edge_count=0,
                conflict_count=0,
                no_signal_count=0,
                inter_scc_ordering_edge_count=0,
                constrained_pair_count=0,
                scc_count=1,
                incomparable_pair_inversion_count=0,
                constrained_inversion_count=0,
                unconstrained_inversion_count=0,
                moved_vertex_count=0,
                unchanged_slice='true',
                position_displacement_median=0,
                position_displacement_p95=0,
            )
        )

        self.assertEqual(record['direct_edge_coverage'], '')
        self.assertEqual(
            record['direct_edge_coverage_status'], 'zero_denominator'
        )
        self.assertEqual(record['moved_vertex_rate'], 0.0)
        self.assertEqual(record['moved_vertex_rate_status'], 'valid')
        self.assertEqual(record['derived_status'], 'valid')

    def test_no_edge_slice_remains_valid_with_metric_specific_status(self):
        record = _parse_mrv_slice_line(
            valid_slice_line(
                eligible_pair_count=2,
                ineligible_pair_count=3,
                edge_count=0,
                conflict_count=1,
                no_signal_count=1,
                intra_scc_ordering_edge_count=0,
                inter_scc_ordering_edge_count=0,
            )
        )

        self.assertEqual(record['derived_status'], 'valid')
        self.assertEqual(record['cross_scc_edge_rate'], '')
        self.assertEqual(
            record['cross_scc_edge_rate_status'], 'zero_denominator'
        )
        self.assertNotIn('edge_pair_rate', record)

    def test_rejects_invalid_inversion_decomposition(self):
        with self.assertRaisesRegex(ValueError, 'inversion decomposition'):
            _parse_mrv_slice_line(
                valid_slice_line(
                    constrained_inversion_count=0,
                    unconstrained_inversion_count=0,
                )
            )

    def test_pair_identity_failure_is_malformed_input(self):
        with self.assertRaisesRegex(ValueError, 'all_pair_count'):
            _parse_mrv_slice_line(valid_slice_line(all_pair_count=7))

    def test_rejects_inconsistent_delay_displacement_order_and_scc_fields(self):
        invalid = (
            (
                {'position_displacement_median': -0.1},
                'normalized displacement',
            ),
            (
                {'position_displacement_p95': 1.1},
                'normalized displacement',
            ),
            (
                {
                    'position_displacement_median': 0.8,
                    'position_displacement_p95': 0.7,
                },
                'normalized displacement',
            ),
            (
                {'unchanged_slice': 'true', 'moved_vertex_count': 2},
                'unchanged_slice',
            ),
            ({'scc_count': 5}, 'scc_count'),
            ({'scc_count': 3}, 'SCC vertex accounting'),
            ({'seal_wait_rounds': 3}, 'seal_wait_rounds'),
            ({'release_delay_ms': 26}, 'release_delay_ms'),
        )
        for overrides, message in invalid:
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, message):
                    _parse_mrv_slice_line(valid_slice_line(**overrides))

    def test_primary_parser_distinguishes_unavailable_and_malformed(self):
        config_log = valid_primary_config_log()
        parser = LogParser.__new__(LogParser)

        unavailable = parser._parse_primaries(config_log)[3]
        malformed = parser._parse_primaries(
            f'{config_log}\n{valid_slice_line(all_pair_count=7)}'
        )[3]

        self.assertEqual(unavailable[0]['data_status'], 'unavailable')
        self.assertEqual(malformed[0]['data_status'], 'malformed')
        self.assertIn('all_pair_count', malformed[0]['error'])

    def test_primary_parser_merges_release_delay_into_sealed_slice(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                registered_slice_line(),
                valid_slice_line(),
                'MRV_SliceRelease slice_id=7 release_delay_ms=35',
            )
        )

        records = parser._parse_primaries(log)[3]

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['data_status'], 'valid')
        self.assertEqual(records[0]['release_delay_ms'], 35)
        self.assertEqual(records[0]['release_delay_status'], 'valid')

    def test_primary_parser_marks_unsealed_registration_right_censored(self):
        parser = LogParser.__new__(LogParser)
        records = parser._parse_primaries(
            f'{valid_primary_config_log()}\n{registered_slice_line()}'
        )[3]

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['data_status'], 'right_censored')
        self.assertEqual(records[0]['slice_id'], 7)
        self.assertEqual(records[0]['slice_size'], 4)
        self.assertEqual(records[0]['seal_horizon'], 14)
        self.assertEqual(records[0]['leader_round'], 10)
        self.assertEqual(
            records[0]['export_prefix_digest'],
            'ZXhwb3J0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        )

    def test_primary_parser_preserves_exact_once_marker_when_primary_panics(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                '[ERROR] MRV_ExactOnceViolation first_slice_id=3 '
                'duplicate_slice_id=7 '
                'member_digest=bWVtYmVyAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= '
                'member_round=11 '
                'member_creator=Y3JlYXRvcgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
                "thread 'tokio-runtime-worker' panicked",
            )
        )

        records = parser._parse_primaries(log)[3]

        self.assertEqual(records[0]['data_status'], 'diagnostic_failure')
        self.assertEqual(records[0]['mismatch_class'], 'EXACT_ONCE_VIOLATION')
        self.assertEqual(records[0]['slice_id'], 7)
        self.assertEqual(records[0]['first_slice_id'], 3)
        self.assertEqual(records[0]['duplicate_slice_id'], 7)
        self.assertEqual(records[0]['member_round'], 11)

    def test_primary_parser_preserves_lifecycle_reason_when_primary_panics(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                '[ERROR] MRV_MemberLifecycleViolation '
                'reason=missing_unsealed_auf_state slice_id=8 '
                'member_digest=bWVtYmVyAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
                "thread 'tokio-runtime-worker' panicked",
            )
        )

        record = parser._parse_primaries(log)[3][0]

        self.assertEqual(record['data_status'], 'diagnostic_failure')
        self.assertEqual(record['mismatch_class'], 'MEMBER_LIFECYCLE_VIOLATION')
        self.assertEqual(record['lifecycle_reason'], 'missing_unsealed_auf_state')

    def test_cross_replica_classification_separates_base_prefix_and_execution(self):
        records = []
        for slice_id in (1, 2, 3, 4, 5):
            for replica_index in (0, 1):
                record = _parse_mrv_slice_line(
                    valid_slice_line(slice_id=slice_id)
                )
                record.update(
                    {
                        'experiment_id': 'experiment-a',
                        'deployment': 'remote',
                        'node_count': 2,
                        'input_rate': 50_000,
                        'faults': 0,
                        'run_index': 1,
                        'replica_index': replica_index,
                        'leader_round': 10,
                        'leader_digest': 'leader=',
                        'member_set_digest': 'set=',
                        'member_order_digest': 'order=',
                        'cumulative_member_count': 20,
                        'export_prefix_digest': 'export=',
                    }
                )
                if slice_id == 1 and replica_index == 1:
                    record['member_order_digest'] = 'different-order='
                if slice_id == 2 and replica_index == 1:
                    record['trigger_export_prefix_digest'] = 'different-prefix='
                if slice_id == 3 and replica_index == 1:
                    record['execution_order_digest'] = 'different-execution='
                if slice_id == 4 and replica_index == 1:
                    record['leader_digest'] = 'different-leader='
                if slice_id == 5 and replica_index == 1:
                    record['member_set_digest'] = 'different-set='
                records.append(record)

        _classify_mrv_cross_replica(records)

        classes = {
            slice_id: {
                record['mismatch_class']
                for record in records
                if record['slice_id'] == slice_id
            }
            for slice_id in (1, 2, 3, 4, 5)
        }
        self.assertEqual(classes[1], {'BASE_ORDER_MISMATCH'})
        self.assertEqual(classes[2], {'SEAL_PREFIX_MISMATCH'})
        self.assertEqual(classes[3], {'EXECUTION_ORDER_MISMATCH'})
        self.assertEqual(classes[4], {'LEADER_MAPPING_MISMATCH'})
        self.assertEqual(classes[5], {'MEMBER_SET_MISMATCH'})
        self.assertTrue(
            all(
                not record['snapshot_frontier_mismatch']
                for record in records
                if record['slice_id'] == 2
            )
        )

    def test_cross_replica_classification_does_not_call_incomplete_consistent(self):
        record = _parse_mrv_slice_line(valid_slice_line())
        record.update(
            {
                'experiment_id': 'experiment-a',
                'deployment': 'remote',
                'node_count': 2,
                'input_rate': 50_000,
                'faults': 0,
                'run_index': 1,
                'replica_index': 0,
            }
        )

        _classify_mrv_cross_replica([record])

        self.assertEqual(record['cross_replica_status'], 'incomplete')

    def test_primary_parser_rejects_registration_stats_mismatch(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                registered_slice_line(slice_size=5),
                valid_slice_line(),
            )
        )

        record = parser._parse_primaries(log)[3][0]

        self.assertEqual(record['data_status'], 'malformed')
        self.assertIn('registration fields disagree', record['error'])

    def test_primary_parser_marks_duplicate_release_malformed(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                valid_slice_line(),
                'MRV_SliceRelease slice_id=7 release_delay_ms=35',
                'MRV_SliceRelease slice_id=7 release_delay_ms=36',
            )
        )

        record = parser._parse_primaries(log)[3][0]

        self.assertEqual(record['data_status'], 'malformed')
        self.assertIn('duplicate release', record['error'])

    def test_primary_parser_marks_all_duplicate_slice_records_malformed(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                valid_slice_line(),
                valid_slice_line(),
            )
        )

        records = parser._parse_primaries(log)[3]
        self.assertEqual(len(records), 2)
        self.assertTrue(all(x['data_status'] == 'malformed' for x in records))
        self.assertTrue(all('duplicate slice_id 7' in x['error'] for x in records))

    def test_primary_parser_rejects_release_before_sealing(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                valid_slice_line(),
                'MRV_SliceRelease slice_id=7 release_delay_ms=26',
            )
        )

        record = parser._parse_primaries(log)[3][0]

        self.assertEqual(record['data_status'], 'malformed')
        self.assertIn('precedes seal_delay_ms', record['error'])

    def test_primary_parser_marks_unknown_and_invalid_release_malformed(self):
        parser = LogParser.__new__(LogParser)
        log = '\n'.join(
            (
                valid_primary_config_log(),
                valid_slice_line(),
                'MRV_SliceRelease slice_id=9 release_delay_ms=35',
                'MRV_SliceRelease slice_id=7 release_delay_ms=-1',
            )
        )

        records = parser._parse_primaries(log)[3]
        malformed = [x for x in records if x['data_status'] == 'malformed']

        self.assertEqual(len(malformed), 2)
        self.assertTrue(any('unknown release slice_id 9' in x['error'] for x in malformed))
        self.assertTrue(any('negative slice id or release delay' in x['error'] for x in malformed))

    def test_node_window_default_does_not_read_gc_depth(self):
        parameters = {
            'header_size': 1_000,
            'max_header_delay': 200,
            'gc_depth': 9,
            'sync_retry_delay': 10_000,
            'sync_retry_nodes': 3,
            'batch_size': 500_000,
            'max_batch_delay': 200,
        }
        node_parameters = NodeParameters(parameters)

        self.assertEqual(node_parameters.mrv_windows, [DEFAULT_MRV_WINDOW])
        self.assertNotEqual(node_parameters.mrv_windows[0], parameters['gc_depth'])

    def test_sweep_prints_one_scalar_and_csv_appends_one_header(self):
        parameters = {
            'header_size': 1_000,
            'max_header_delay': 200,
            'gc_depth': 50,
            'mrv_window': [2, 4],
            'sync_retry_delay': 10_000,
            'sync_retry_nodes': 3,
            'batch_size': 500_000,
            'max_batch_delay': 200,
        }
        node_parameters = NodeParameters(parameters)
        record = _parse_mrv_slice_line(valid_slice_line())
        record.update(
            {
                'deployment': 'local',
                'experiment_id': 'experiment-a',
                'node_count': 4,
                'input_rate': 50_000,
                'client_target_rate': 50_000,
                'faults': 0,
                'run_index': 1,
                'replica_index': 0,
                'source_log': 'primary-0.log',
            }
        )

        with tempfile.TemporaryDirectory() as directory:
            parameters_file = os.path.join(directory, 'parameters.json')
            node_parameters.print(parameters_file, mrv_window=4)
            with open(parameters_file, 'r') as f:
                written_parameters = json.load(f)
            self.assertEqual(written_parameters['mrv_window'], 4)
            self.assertIsInstance(written_parameters['mrv_window'], int)

            parser = LogParser.__new__(LogParser)
            parser.mrv_slice_records = [record]
            csv_file = os.path.join(directory, 'mrv.csv')
            parser.print_mrv(csv_file)
            parser.print_mrv(csv_file)

            with open(csv_file, newline='') as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]['deployment'], 'local')
            self.assertEqual(rows[0]['experiment_id'], 'experiment-a')
            self.assertEqual(rows[0]['mrv_window'], '4')

    def test_csv_rejects_old_schema_instead_of_misaligning_rows(self):
        parser = LogParser.__new__(LogParser)
        parser.mrv_slice_records = []

        with tempfile.TemporaryDirectory() as directory:
            csv_file = os.path.join(directory, 'mrv.csv')
            with open(csv_file, 'w', newline='') as f:
                f.write('deployment,mrv_window\n')

            with self.assertRaisesRegex(ParseError, 'schema differs'):
                parser.print_mrv(csv_file)

    def test_bench_parameters_preserve_or_generate_experiment_id(self):
        parameters = {
            'faults': 0,
            'nodes': 4,
            'workers': 1,
            'rate': 50_000,
            'tx_size': 512,
            'duration': 20,
            'drain_duration': 30,
            'experiment_id': 'experiment-a',
        }

        self.assertEqual(
            BenchParameters(parameters).experiment_id, 'experiment-a'
        )
        generated = dict(parameters)
        del generated['experiment_id']
        self.assertTrue(BenchParameters(generated).experiment_id)

    def test_plot_paths_keep_windows_separate(self):
        window_2 = PathMaker.plot_file('latency', 'png', mrv_window=2)
        window_4 = PathMaker.plot_file('latency', 'png', mrv_window=4)

        self.assertNotEqual(window_2, window_4)
        self.assertTrue(window_2.endswith(os.path.join('plots', 'mrv-latency-w2.png')))
        self.assertTrue(window_4.endswith(os.path.join('plots', 'mrv-latency-w4.png')))


if __name__ == '__main__':
    unittest.main()
