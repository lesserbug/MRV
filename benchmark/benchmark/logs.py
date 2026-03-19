# Copyright(C) Facebook, Inc. and its affiliates.
from collections import Counter, defaultdict
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    WAVE_ID_SHIFT = 48

    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers = len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        (
            self.size,
            self.rate,
            self.start,
            misses,
            self.sent_samples,
            workloads,
            wave_bursts,
            wave_gaps,
        ) = zip(*results)
        self.misses = sum(misses)
        self.workload = workloads[0]
        self.wave_burst_ms = next((x for x in wave_bursts if x is not None), None)
        self.wave_gap_ms = next((x for x in wave_gaps if x is not None), None)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        (
            proposals,
            batch_headers,
            consensus_commits,
            execution_commits,
            batch_stats,
            pair_stats,
            tusk_auf_orders,
            mrv_auf_orders,
            self.configs,
            primary_ips,
        ) = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.batch_headers = self._merge_maps(batch_headers)
        self.consensus_commits = self._merge_results([x.items() for x in consensus_commits])
        self.execution_commits = self._merge_results([x.items() for x in execution_commits])
        self.batch_stats = self._merge_batch_stats(batch_stats)
        self.pair_stats = self._merge_pair_stats(pair_stats)
        self.tusk_auf_order = self._select_longest_sequence(tusk_auf_orders)
        self.mrv_auf_order = self._select_longest_sequence(mrv_auf_orders)
        self.delta_threshold = self._delta_threshold()

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, batch_samples, workers_ips = zip(*results)
        committed = set(self.consensus_commits) | set(self.execution_commits)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in committed
        }
        self.batch_samples = self._merge_batch_samples(batch_samples)

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

        self.activation_metrics = self._compute_activation_metrics()
        self.oracle_activation_metrics = self._compute_oracle_activation_metrics()
        self.wave_metrics = self._compute_wave_metrics()

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if k not in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _merge_batch_stats(self, stats_by_log):
        merged = {}
        for batch_stats in stats_by_log:
            for batch, stats in batch_stats.items():
                if batch not in merged:
                    merged[batch] = stats
        return merged

    def _merge_maps(self, mappings):
        merged = {}
        for mapping in mappings:
            for key, value in mapping.items():
                if key not in merged:
                    merged[key] = value
        return merged

    def _merge_pair_stats(self, pair_stats_by_log):
        merged = {}
        for pair_stats in pair_stats_by_log:
            for key, value in pair_stats.items():
                if key not in merged:
                    merged[key] = value
        return merged

    def _merge_batch_samples(self, batch_samples):
        merged = defaultdict(set)
        for samples in batch_samples:
            for batch, tx_ids in samples.items():
                merged[batch].update(tx_ids)
        return dict(merged)

    def _select_longest_sequence(self, sequences):
        return list(max(sequences, key=len, default=[]))

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))
        workload = search(r'Workload: (steady|waves)', log)
        wave_burst = search(r'Wave burst: (\d+) ms', log)
        wave_gap = search(r'Wave gap: (\d+) ms', log)

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return (
            size,
            rate,
            start,
            misses,
            samples,
            workload.group(1) if workload else 'steady',
            int(wave_burst.group(1)) if wave_burst else None,
            int(wave_gap.group(1)) if wave_gap else None,
        )

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created (B\d+\([^ ]+\)) -> ([^ ]+=)', log)
        proposals = self._merge_results([[(d, self._to_posix(t)) for t, _, d in tmp]])
        batch_headers = {d: h for _, h, d in tmp}

        tmp = findall(r'\[(.*Z) .* Tusk_Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        consensus_commits = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* MRV_Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        execution_commits = self._merge_results([tmp])

        batch_stats = {}
        for line in findall(r'MRV_BatchStats ([^\n]+)', log):
            fields = dict(findall(r'(\w+)=([^\s]+)', line))
            batch = int(fields['batch'])
            raw_scc_sizes = fields.get('scc_sizes', '[]')[1:-1]
            batch_stats[batch] = {
                'size': int(fields['size']),
                'matured_aufs': int(fields['matured_aufs']),
                'total_pairs': int(fields['total_pairs']),
                'matured_pairs': int(fields['matured_pairs']),
                'trunc_pairs': int(fields['trunc_pairs']),
                'conflict_pairs': int(fields.get('conflict_pairs', 0)),
                'no_signal_pairs': int(fields.get('no_signal_pairs', 0)),
                'edges': int(fields['edges']),
                'fair_pairs': int(fields.get('fair_pairs', 0)),
                'implied_pairs': int(fields['implied_pairs']),
                'tie_pairs': int(fields['tie_pairs']),
                'nontrivial_scc_nodes': int(fields['nontrivial_scc_nodes']),
                'max_scc': int(fields['max_scc']),
                'scc_sizes': [] if raw_scc_sizes == '' else [
                    int(x) for x in raw_scc_sizes.split(',')
                ],
            }

        pair_stats = {}
        for line in findall(r'MRV_PairStats ([^\n]+)', log):
            fields = dict(findall(r'(\w+)=([^\s]+)', line))
            key = (int(fields['batch']), fields['a'], fields['b'])
            raw_delta = fields.get('max_abs_delta', 'na')
            pair_stats[key] = {
                'outcome': fields['outcome'],
                'max_abs_delta': None if raw_delta == 'na' else int(raw_delta),
            }

        tusk_auf_order = findall(r'Tusk_AUF_Committed (B\d+\([^ ]+\))', log)
        mrv_auf_order = findall(r'MRV_AUF_Committed (B\d+\([^ ]+\))', log)

        if not consensus_commits and not execution_commits:
            tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
            tmp = [(d, self._to_posix(t)) for t, d in tmp]
            legacy_commits = self._merge_results([tmp])
            consensus_commits = legacy_commits
            execution_commits = legacy_commits

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return (
            proposals,
            batch_headers,
            consensus_commits,
            execution_commits,
            batch_stats,
            pair_stats,
            tusk_auf_order,
            mrv_auf_order,
            configs,
            ip,
        )

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}
        batch_samples = defaultdict(set)
        for digest, tx_id in tmp:
            batch_samples[digest].add(int(tx_id))

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, dict(batch_samples), ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _committed_bytes(self, commits):
        return sum(self.sizes[d] for d in commits if d in self.sizes)

    def _consensus_throughput(self):
        if not self.consensus_commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.consensus_commits.values())
        duration = end - start
        bytes = self._committed_bytes(self.consensus_commits)
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.consensus_commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.execution_commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.execution_commits.values())
        duration = end - start
        bytes = self._committed_bytes(self.execution_commits)
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.execution_commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.execution_commits[batch_id]
                    latency += [end - start]
        return mean(latency) if latency else 0

    def _mrv_post_commit_latency(self):
        latency = [
            self.execution_commits[d] - self.consensus_commits[d]
            for d in self.execution_commits
            if d in self.consensus_commits
        ]
        return mean(latency) if latency else 0

    def _safe_div(self, numerator, denominator):
        return numerator / denominator if denominator else 0

    def _delta_threshold(self):
        if not isinstance(self.committee_size, int) or self.committee_size <= 0:
            return None
        faults = (self.committee_size - 1) // 3
        return faults + 1

    def _decode_wave_id(self, tx_id):
        return tx_id >> self.WAVE_ID_SHIFT

    def _digest_wave_id(self, digest):
        waves = {
            self._decode_wave_id(tx_id)
            for tx_id in self.batch_samples.get(digest, set())
        }
        return next(iter(waves)) if len(waves) == 1 else None

    def _partition_order(self, order):
        partitions = {}
        offset = 0
        for batch in sorted(self.batch_stats):
            size = self.batch_stats[batch]['size']
            if offset + size > len(order):
                break
            partitions[batch] = order[offset:offset + size]
            offset += size
        return partitions

    def _compute_activation_metrics(self):
        if not self.batch_stats:
            return {
                'batch_count': 0,
                'avg_batch_size': 0,
                'matured_auf_ratio': 0,
                'matured_pair_coverage': 0,
                'truncation_abstain_ratio': 0,
                'conflict_abstain_ratio': 0,
                'no_signal_abstain_ratio': 0,
                'mature_mrv_edge_ratio': 0,
                'fairness_implied_pair_ratio': 0,
                'tie_break_only_pair_ratio': 0,
                'nontrivial_scc_ratio': 0,
                'max_scc_size': 0,
                'scc_distribution': Counter(),
                'totals': {},
            }

        totals = defaultdict(int)
        scc_distribution = Counter()
        for stats in self.batch_stats.values():
            for key in (
                'size',
                'matured_aufs',
                'total_pairs',
                'matured_pairs',
                'trunc_pairs',
                'conflict_pairs',
                'no_signal_pairs',
                'edges',
                'fair_pairs',
                'implied_pairs',
                'tie_pairs',
                'nontrivial_scc_nodes',
            ):
                totals[key] += stats[key]
            totals['batch_count'] += 1
            totals['max_scc_size'] = max(totals['max_scc_size'], stats['max_scc'])
            scc_distribution.update(stats['scc_sizes'])

        return {
            'batch_count': totals['batch_count'],
            'avg_batch_size': self._safe_div(totals['size'], totals['batch_count']),
            'matured_auf_ratio': self._safe_div(totals['matured_aufs'], totals['size']),
            'matured_pair_coverage': self._safe_div(totals['matured_pairs'], totals['total_pairs']),
            'truncation_abstain_ratio': self._safe_div(totals['trunc_pairs'], totals['total_pairs']),
            'conflict_abstain_ratio': self._safe_div(totals['conflict_pairs'], totals['total_pairs']),
            'no_signal_abstain_ratio': self._safe_div(totals['no_signal_pairs'], totals['total_pairs']),
            'mature_mrv_edge_ratio': self._safe_div(totals['edges'], totals['matured_pairs']),
            'fairness_implied_pair_ratio': self._safe_div(totals['implied_pairs'], totals['total_pairs']),
            'tie_break_only_pair_ratio': self._safe_div(totals['tie_pairs'], totals['total_pairs']),
            'nontrivial_scc_ratio': self._safe_div(totals['nontrivial_scc_nodes'], totals['size']),
            'max_scc_size': totals['max_scc_size'],
            'scc_distribution': scc_distribution,
            'totals': dict(totals),
        }

    def _compute_oracle_activation_metrics(self):
        metrics = {
            'oracle_pair_count': 0,
            'oracle_matured_pairs': 0,
            'oracle_edges': 0,
            'oracle_no_signal': 0,
            'oracle_conflict': 0,
            'oracle_tie_break': 0,
            'oracle_delta_bucket_total': 0,
            'delta_eq_0': 0,
            'delta_eq_1': 0,
            'delta_ge_threshold': 0,
        }

        if self.workload != 'waves' or not self.pair_stats:
            metrics['oracle_matured_pair_coverage'] = 0
            metrics['oracle_edge_ratio'] = 0
            metrics['oracle_no_signal_ratio'] = 0
            metrics['oracle_tie_break_ratio'] = 0
            return metrics

        for (_, a, b), stats in self.pair_stats.items():
            wave_a = self._digest_wave_id(a)
            wave_b = self._digest_wave_id(b)
            if wave_a is None or wave_b is None or wave_a == wave_b:
                continue

            metrics['oracle_pair_count'] += 1
            outcome = stats['outcome']
            if outcome != 'truncated':
                metrics['oracle_matured_pairs'] += 1

            if outcome in ('a_before_b', 'b_before_a'):
                metrics['oracle_edges'] += 1
            elif outcome == 'no_signal':
                metrics['oracle_no_signal'] += 1
            elif outcome == 'conflict':
                metrics['oracle_conflict'] += 1

            if outcome != 'a_before_b' and outcome != 'b_before_a':
                metrics['oracle_tie_break'] += 1

            max_abs_delta = stats.get('max_abs_delta')
            if max_abs_delta is None:
                continue

            metrics['oracle_delta_bucket_total'] += 1
            if max_abs_delta == 0:
                metrics['delta_eq_0'] += 1
            elif max_abs_delta == 1:
                metrics['delta_eq_1'] += 1
            elif self.delta_threshold is not None and max_abs_delta >= self.delta_threshold:
                metrics['delta_ge_threshold'] += 1

        metrics['oracle_matured_pair_coverage'] = self._safe_div(
            metrics['oracle_matured_pairs'], metrics['oracle_pair_count']
        )
        metrics['oracle_edge_ratio'] = self._safe_div(
            metrics['oracle_edges'], metrics['oracle_pair_count']
        )
        metrics['oracle_no_signal_ratio'] = self._safe_div(
            metrics['oracle_no_signal'], metrics['oracle_pair_count']
        )
        metrics['oracle_tie_break_ratio'] = self._safe_div(
            metrics['oracle_tie_break'], metrics['oracle_pair_count']
        )
        return metrics

    def _compute_wave_metrics(self):
        metrics = {
            'enabled': self.workload == 'waves',
            'covered_batches': 0,
            'wave_pure_aufs': 0,
            'mixed_wave_aufs': 0,
            'wave_oracle_pairs': 0,
            'tusk_wave_inversions': 0,
            'mrv_wave_inversions': 0,
            'tusk_wave_inversion_rate': 0,
            'mrv_wave_inversion_rate': 0,
        }

        if self.workload != 'waves':
            return metrics

        tusk_batches = self._partition_order(self.tusk_auf_order)
        mrv_batches = self._partition_order(self.mrv_auf_order)
        if not tusk_batches or not mrv_batches:
            return metrics

        header_waves = defaultdict(set)
        for digest, header in self.batch_headers.items():
            for tx_id in self.batch_samples.get(digest, set()):
                header_waves[header].add(self._decode_wave_id(tx_id))

        for batch in sorted(set(tusk_batches) & set(mrv_batches)):
            tusk_headers = tusk_batches[batch]
            mrv_headers = mrv_batches[batch]
            if len(tusk_headers) != len(mrv_headers):
                continue

            metrics['covered_batches'] += 1
            tusk_positions = {header: idx for idx, header in enumerate(tusk_headers)}
            mrv_positions = {header: idx for idx, header in enumerate(mrv_headers)}

            pure_headers = {}
            for header in tusk_headers:
                waves = header_waves.get(header, set())
                if len(waves) == 1:
                    pure_headers[header] = next(iter(waves))
                elif len(waves) > 1:
                    metrics['mixed_wave_aufs'] += 1

            headers = sorted(
                pure_headers,
                key=lambda header: tusk_positions.get(header, len(tusk_headers)),
            )
            metrics['wave_pure_aufs'] += len(headers)

            for i, first in enumerate(headers):
                for second in headers[i + 1:]:
                    first_wave = pure_headers[first]
                    second_wave = pure_headers[second]
                    if first_wave == second_wave:
                        continue

                    metrics['wave_oracle_pairs'] += 1
                    if first_wave < second_wave:
                        early, late = first, second
                    else:
                        early, late = second, first

                    if tusk_positions[early] > tusk_positions[late]:
                        metrics['tusk_wave_inversions'] += 1
                    if mrv_positions[early] > mrv_positions[late]:
                        metrics['mrv_wave_inversions'] += 1

        metrics['tusk_wave_inversion_rate'] = self._safe_div(
            metrics['tusk_wave_inversions'], metrics['wave_oracle_pairs']
        )
        metrics['mrv_wave_inversion_rate'] = self._safe_div(
            metrics['mrv_wave_inversions'], metrics['wave_oracle_pairs']
        )
        return metrics

    def _format_ratio(self, numerator, denominator, precision=2):
        ratio = 100 * self._safe_div(numerator, denominator)
        return f'{ratio:.{precision}f}% ({numerator:,}/{denominator:,})'

    def _format_scc_distribution(self):
        distribution = self.activation_metrics['scc_distribution']
        if not distribution:
            return 'n/a'
        return ', '.join(
            f'{size}:{distribution[size]}'
            for size in sorted(distribution)
        )

    def _format_delta_buckets(self):
        oracle = self.oracle_activation_metrics
        total = oracle['oracle_delta_bucket_total']
        if total == 0:
            return 'n/a'

        threshold_label = (
            f'>={self.delta_threshold}'
            if self.delta_threshold is not None else '>=delta'
        )
        return ', '.join([
            f'0:{oracle["delta_eq_0"]}/{total}',
            f'1:{oracle["delta_eq_1"]}/{total}',
            f'{threshold_label}:{oracle["delta_ge_threshold"]}/{total}',
        ])

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000
        mrv_post_commit_latency = self._mrv_post_commit_latency() * 1_000
        activation = self.activation_metrics
        oracle = self.oracle_activation_metrics
        wave = self.wave_metrics
        totals = activation['totals']

        workload_lines = f' Workload: {self.workload}\n'
        if self.workload == 'waves':
            workload_lines += f' Wave burst: {self.wave_burst_ms:,} ms\n'
            workload_lines += f' Wave gap: {self.wave_gap_ms:,} ms\n'

        fairness_lines = (
            ' + FAIRNESS:\n'
            f' Finalized MRV batches: {activation["batch_count"]:,}\n'
            f' Mean batch size: {activation["avg_batch_size"]:.2f} AUF(s)\n'
        )

        if activation['batch_count'] == 0:
            fairness_lines += ' No MRV_BatchStats found in the logs.\n'
        else:
            fairness_lines += (
                f' Matured AUF ratio: {self._format_ratio(totals["matured_aufs"], totals["size"])}\n'
                f' Matured pair coverage: {self._format_ratio(totals["matured_pairs"], totals["total_pairs"])}\n'
                f' Truncation abstain ratio: {self._format_ratio(totals["trunc_pairs"], totals["total_pairs"])}\n'
                f' Mature MRV edge ratio: {self._format_ratio(totals["edges"], totals["matured_pairs"])}\n'
                f' Fairness implied pair ratio: {self._format_ratio(totals["implied_pairs"], totals["total_pairs"])}\n'
                f' Tie-break-only pair ratio: {self._format_ratio(totals["tie_pairs"], totals["total_pairs"])}\n'
                f' Nontrivial SCC ratio: {self._format_ratio(totals["nontrivial_scc_nodes"], totals["size"])}\n'
                f' Max SCC size: {activation["max_scc_size"]:,}\n'
                f' SCC size distribution: {self._format_scc_distribution()}\n'
            )
            if totals.get('conflict_pairs', 0) or totals.get('no_signal_pairs', 0):
                fairness_lines += (
                    f' Conflict abstain ratio: {self._format_ratio(totals["conflict_pairs"], totals["total_pairs"])}\n'
                    f' No-signal abstain ratio: {self._format_ratio(totals["no_signal_pairs"], totals["total_pairs"])}\n'
                )

        if wave['enabled']:
            fairness_lines += (
                f' Wave oracle batches: {wave["covered_batches"]:,}\n'
                f' Wave-pure AUFs: {wave["wave_pure_aufs"]:,}\n'
                f' Mixed-wave AUFs: {wave["mixed_wave_aufs"]:,}\n'
                f' Wave oracle pairs: {wave["wave_oracle_pairs"]:,}\n'
                f' Oracle matured pair coverage: {self._format_ratio(oracle["oracle_matured_pairs"], oracle["oracle_pair_count"])}\n'
                f' Oracle edge ratio: {self._format_ratio(oracle["oracle_edges"], oracle["oracle_pair_count"])}\n'
                f' Oracle no-signal ratio: {self._format_ratio(oracle["oracle_no_signal"], oracle["oracle_pair_count"])}\n'
                f' Oracle tie-break-only ratio: {self._format_ratio(oracle["oracle_tie_break"], oracle["oracle_pair_count"])}\n'
                f' Oracle max|delta| buckets: {self._format_delta_buckets()}\n'
                f' Tusk wave inversion rate: {self._format_ratio(wave["tusk_wave_inversions"], wave["wave_oracle_pairs"])}\n'
                f' MRV wave inversion rate: {self._format_ratio(wave["mrv_wave_inversions"], wave["wave_oracle_pairs"])}\n'
            )
        else:
            fairness_lines += ' Wave inversion rate: n/a (steady workload)\n'

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            f'{workload_lines}'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            f' MRV post-commit latency: {round(mrv_post_commit_latency):,} ms\n'
            '\n'
            f'{fairness_lines}'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)
