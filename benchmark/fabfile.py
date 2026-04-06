# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError


@task
def local(
    ctx,
    debug=True,
    nodes=5,
    workers=1,
    faults=0,
    rate='auto',
    per_worker_rate=12_500,
    workload='steady',
    wave_burst_ms=300,
    wave_gap_ms=1200,
    skew_ms=200,
    fairness=False,
):
    ''' Run benchmarks on localhost '''
    nodes = int(nodes)
    workers = int(workers)
    faults = int(faults)
    per_worker_rate = int(per_worker_rate)
    total_workers = nodes * workers
    rate = total_workers * per_worker_rate if str(rate) == 'auto' else int(rate)
    fairness = bool(int(fairness)) if isinstance(fairness, str) else bool(fairness)

    bench_params = {
        'faults': faults,
        'nodes': nodes,
        'workers': workers,
        'rate': rate,
        'tx_size': 512,
        'workload': workload,
        'wave_burst_ms': int(wave_burst_ms),
        'wave_gap_ms': int(wave_gap_ms),
        'skew_ms': int(skew_ms),
        'duration': 20,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result(include_fairness=fairness))
    except BenchError as e:
        Print.error(e)


@task
def attack_local(
    ctx,
    debug=True,
    nodes=5,
    workers=1,
    faults=0,
    rate='auto',
    per_worker_rate=12_500,
    wave_burst_ms=300,
    wave_gap_ms=400,
    skew_ms=200,
):
    ''' Run a local skewed-dissemination fairness diagnostic '''
    bench_params = {
        'faults': int(faults),
        'nodes': int(nodes),
        'workers': int(workers),
        'rate': (
            int(nodes) * int(workers) * int(per_worker_rate)
            if str(rate) == 'auto' else int(rate)
        ),
        'tx_size': 512,
        'workload': 'skewed_waves',
        'wave_burst_ms': int(wave_burst_ms),
        'wave_gap_ms': int(wave_gap_ms),
        'skew_ms': int(skew_ms),
        'duration': 20,
    }
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result(include_fairness=True))
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False, workload='steady', wave_burst_ms=300, wave_gap_ms=1200, skew_ms=200, fairness=True):
    ''' Run benchmarks on AWS '''
    fairness = bool(int(fairness)) if isinstance(fairness, str) else bool(fairness)
    bench_params = {
        'faults': 3,
        'nodes': [10],
        'workers': 1,
        'collocate': True,
        'rate': [10_000, 110_000],
        'tx_size': 512,
        'workload': workload,
        'wave_burst_ms': int(wave_burst_ms),
        'wave_gap_ms': int(wave_gap_ms),
        'skew_ms': int(skew_ms),
        'duration': 300,
        'runs': 2,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug, include_fairness=fairness)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx, fairness=False):
    ''' Print a summary of the logs '''
    try:
        fairness = bool(int(fairness)) if isinstance(fairness, str) else bool(fairness)
        print(LogParser.process('./logs', faults='?').result(include_fairness=fairness))
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
