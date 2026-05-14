
# MRV: Post-Consensus Structural Ordering for DAG-BFT

MRV is a research prototype that extends a Narwhal/Tusk-style DAG-BFT stack with a post-consensus structural ordering layer. The codebase is based on the open-source Narwhal/Tusk implementation and adds an `mrv_executor` layer between consensus delivery and execution.

MRV does not modify Narwhal's worker dissemination path, Tusk's voting logic, or the consensus commit rule. Instead, after Tusk commits a DAG output, MRV interprets the committed DAG structure to derive a deterministic, slice-local order over atomic units of fairness (AUFs). It uses authenticated creator, round, and ancestry metadata already present in the committed DAG.

This repository is intended for research and benchmarking. It is not production software.

## What MRV Adds

The main addition is the `mrv_executor` crate. It receives committed DAG outputs from the consensus layer, constructs committed execution slices, and orders AUFs using MRV's structural evidence rules.

At a high level, MRV performs four steps:

1. **Committed-slice extraction**: identify newly delivered AUFs from each committed DAG output.
2. **Creator-level visibility tracking**: count which creators' committed AUFs see each target AUF through DAG ancestry.
3. **Pairwise verdicts**: add an evidence-backed precedence edge only when a mature AUF pair has a one-sided Byzantine-robust visibility signal.
4. **Graph linearization**: assemble causal and evidence-backed constraints, condense SCCs, and use deterministic completion only for residual ambiguity.

MRV is conservative by design. If the committed DAG does not provide mature, one-sided evidence for a pair, MRV abstains and resolves the pair only through deterministic completion.

## Repository Layout

```text
.
├── benchmark/       # Local and AWS benchmark harnesses
├── config/          # Protocol and benchmark configuration
├── consensus/       # Tusk consensus logic
├── crypto/          # Cryptographic primitives
├── mrv_executor/    # MRV post-consensus ordering layer
├── network/         # Networking utilities
├── node/            # Node binary and MRV pipeline wiring
├── primary/         # Narwhal primary
├── store/           # RocksDB-backed storage
└── worker/          # Narwhal worker
```

The MRV pipeline is wired in `node/src/main.rs`: consensus outputs are sent to `MrvExecutor`, and the benchmark analyzer observes MRV's ordered output.

## Requirements

The core protocol is written in Rust. Benchmark orchestration is written in Python and uses Fabric.

You need:

- Rust toolchain
- Python 3.9+
- Clang, required by RocksDB
- tmux, used to run local nodes and clients
- AWS credentials, only for remote WAN experiments

Install Python benchmark dependencies:

```bash
cd benchmark
pip install -r requirements.txt
```

## Quick Start: Local Benchmark

Clone the repository and select the MRV branch:

```bash
git clone https://github.com/lesserbug/MRV.git
cd MRV
git checkout mrv-dev2
cd benchmark
pip install -r requirements.txt
```

Run a local benchmark:

```bash
fab local
```

The default local benchmark starts a small committee on the local machine. The first run may take longer because Rust binaries are compiled in release mode.

You can customize the local run from the command line:

```bash
fab local --nodes=5 --workers=1 --faults=0 --per-worker-rate=12500
```

To print MRV fairness and coverage diagnostics, enable the fairness summary:

```bash
fab local --fairness=1
```

## Remote AWS Benchmarking

Remote benchmarks are controlled from the `benchmark` directory through Fabric. Before running AWS experiments, edit `benchmark/settings.json`:

```json
{
    "key": {
        "name": "aws",
        "path": "/path/to/your/aws/key"
    },
    "port": 5000,
    "repo": {
        "name": "MRV",
        "url": "https://github.com/lesserbug/MRV.git",
        "branch": "mrv-dev2"
    },
    "instances": {
        "type": "m5.xlarge",
        "regions":  ["us-west-1", "us-east-1", "ap-northeast-1", "ap-northeast-2", "eu-central-1"]
    }
}
```
This is an active development prototype. If remote experiment orchestration behaves differently across environments, compare the benchmark control scripts under `benchmark/benchmark/` between the `mrv-dev2` branch and the `mrv-dev2-wsl-exp-snapshot-20260514` branch; the latter preserves the WSL-side scripts used for our AWS experiment control.


Create and inspect a remote testbed:

```bash
fab create --nodes=10
fab info
```

Install the codebase on all machines:

```bash
fab install
```

Run the remote benchmark:

```bash
fab remote
```

Stop or destroy the testbed when finished:

```bash
fab stop
fab destroy
```

Use `fab kill` if tmux sessions are still running on the remote machines.

## Benchmark Parameters

The main benchmark parameters are defined in `benchmark/fabfile.py`.

Common parameters include:

- `nodes`: committee size
- `workers`: workers per validator
- `faults`: configured fault-tolerance parameter
- `rate`: offered transaction load
- `tx_size`: transaction size
- `batch_size`: worker batch size
- `max_batch_delay`: maximum worker batch delay
- `max_header_delay`: maximum primary header delay
- `gc_depth`: committed-history retention depth, also used as MRV's observation window cap in this prototype

The default remote benchmark uses a geo-distributed AWS deployment and reports throughput, end-to-end latency, MRV post-commit latency, and optional MRV coverage diagnostics.

## Plotting Results

After remote experiments, use:

```bash
fab plot
```

The plotting code reads benchmark logs from `benchmark/logs/` and generates throughput-latency figures according to the parameters in `benchmark/fabfile.py`.

## License

This software is licensed under Apache 2.0. See `LICENSE`.
```
