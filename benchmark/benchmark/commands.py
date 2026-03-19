# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} primary')

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(
        address,
        size,
        rate,
        nodes,
        workload='steady',
        wave_burst_ms=300,
        wave_gap_ms=1200,
        skew_ms=200,
        client_index=0,
        client_count=1,
    ):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        assert workload in ('steady', 'waves', 'skewed_waves')
        assert isinstance(wave_burst_ms, int) and wave_burst_ms > 0
        assert isinstance(wave_gap_ms, int) and wave_gap_ms > 0
        assert isinstance(skew_ms, int) and skew_ms > 0
        assert isinstance(client_index, int) and client_index >= 0
        assert isinstance(client_count, int) and client_count > 0
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        workload_args = ''
        if workload == 'waves':
            workload_args = (
                f' --workload {workload}'
                f' --wave-burst-ms {wave_burst_ms}'
                f' --wave-gap-ms {wave_gap_ms}'
            )
        elif workload == 'skewed_waves':
            workload_args = (
                f' --workload {workload}'
                f' --wave-burst-ms {wave_burst_ms}'
                f' --wave-gap-ms {wave_gap_ms}'
                f' --skew-ms {skew_ms}'
                f' --client-index {client_index}'
                f' --client-count {client_count}'
            )
        return f'./benchmark_client {address} --size {size} --rate {rate}{workload_args} {nodes}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
