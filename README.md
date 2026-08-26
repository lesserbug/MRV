# MRV

MRV is a research prototype that adds a post-consensus structural ordering
layer to a Narwhal/Tusk-style DAG-BFT stack. It is based on the open-source
[Narwhal and Tusk](https://github.com/asonnino/narwhal) implementation.

MRV does not modify worker dissemination, voting, or the Tusk commit rule.
After Tusk emits a committed sub-DAG, MRV uses authenticated creator and round metadata together with ancestry reconstructed from authenticated parent references to derive a deterministic order within that execution slice. Slice membership and the order between slices remain fixed by Tusk.

This repository is intended for research and benchmarking. It is not
production software.

## Protocol overview

MRV implements a fixed-window structural comparison:

1. **Committed-slice registration.** Tusk `CommittedSubDag` outputs define the
   vertices and native traversal order of each execution slice.
2. **Fixed-snapshot visibility.** MRV reconstructs creator-level ancestry from
   the committed prefix and seals a slice after its fixed horizon, determined
   by the slice maximum round and the configured MRV window `W`.
3. **Eligibility and pair verdicts.** For each causally incomparable pair, MRV
   evaluates eligibility and one-sided visibility differences over the full
   fixed pair window. The prototype uses `q_vis = 2f + 1` and `theta = f + 1`.
4. **Deterministic graph completion.** MRV combines causal and qualifying
   visibility edges, condenses strongly connected components, topologically
   orders the condensation DAG, and uses a fixed key only for residual choices.

MRV is conservative about structural evidence. An ineligible pair, a pair for
which neither direction crosses `theta`, or a pair for which both directions
cross receives no visibility-derived edge. Its residual order is then resolved
by deterministic graph completion.

With `theta = f + 1`, Byzantine creators alone cannot account for a crossing.
The evidence remains structural rather than a transaction receive-order
guarantee, and its ancestry may still reflect message scheduling.

## Repository layout

```text
.
├── benchmark/       # Local and AWS benchmark harnesses
├── config/          # Committee and protocol configuration
├── consensus/       # Unmodified Tusk consensus and committed-DAG export
├── crypto/          # Cryptographic primitives
├── mrv_executor/    # MRV post-consensus ordering layer
├── network/         # Networking utilities
├── node/            # Node binary and MRV pipeline wiring
├── primary/         # Narwhal primary
├── store/           # RocksDB-backed storage
└── worker/          # Narwhal worker
```

The protocol implementation is written in Rust. Benchmark orchestration is
written in Python and uses Fabric.

## Requirements

- A Rust toolchain
- Python 3.9+
- Clang, required when building RocksDB
- `tmux`, used by the local and remote benchmark harnesses
- AWS credentials, only for remote experiments

Clone the MRV branch and install the benchmark dependencies:

```bash
git clone https://github.com/lesserbug/MRV.git
cd MRV
git checkout mrv-dev7
cd benchmark
pip install -r requirements.txt
```

## Build and test

From the repository root, run the MRV test suite with benchmark-only
instrumentation enabled:

```bash
cargo test -p mrv_executor --features benchmark
```

The controlled structural experiments can be run individually:

```bash
cargo test -p mrv_executor --features benchmark \
  controlled_visibility_experiment -- --nocapture

cargo test -p mrv_executor --features benchmark \
  controlled_tusk_vs_mrv_experiment -- --nocapture

cargo test -p mrv_executor --features benchmark \
  controlled_adversarial_structure_experiment -- --nocapture
```

`controlled_adversarial_structure_experiment` is a controlled
parent-availability sensitivity experiment over structurally valid synthetic
DAGs. It conditions on valid first-quorum parent sets and does not emulate a
live network scheduler or estimate attack success in a distributed deployment.
Its aggregate records are printed with the
`MRV_AdversarialStructureStats` marker.

## Local benchmarks

The benchmark parameters are defined in [`benchmark/fabfile.py`](benchmark/fabfile.py).
Edit its `bench_params` and `node_params` dictionaries to select committee
size, offered load, duration, and MRV window. Then run:

```bash
cd benchmark
fab local
```

The `local` task expands an `mrv_window` list into separate fixed-window runs.
It does not accept committee size, rate, or fairness-summary options on the
command line.

Important parameters include:

- `nodes`: committee size
- `workers`: workers per validator
- `faults`: configured fault parameter
- `rate`: offered transaction load
- `tx_size`: transaction size
- `batch_size`: preferred worker batch size
- `max_batch_delay`: maximum worker batch delay
- `max_header_delay`: maximum primary header delay
- `gc_depth`: base-protocol committed-history retention depth
- `mrv_window`: independent fixed MRV comparison window in DAG rounds; a list
  requests a window sweep
- `drain_duration`: time allowed for pending slices to reach final output after
  clients stop submitting transactions

The benchmark feature records per-slice aggregate metrics, including
eligibility, pair verdicts, direct and direct-or-transitive coverage,
constrained inversions relative to Tusk, seal delay, and final execution-order
agreement.

Artifacts are written under:

```text
benchmark/logs/                    # Raw process logs
benchmark/results/                 # Performance summaries
benchmark/results/mrv-slice-stats.csv
benchmark/plots/                   # Generated figures
```

## AWS benchmarks

Before creating a testbed, edit `benchmark/settings.json`. For example:

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
    "branch": "mrv-dev7"
  },
  "instances": {
    "type": "m5.xlarge",
    "regions": [
      "us-east-1",
      "us-west-1",
      "ap-southeast-2",
      "eu-north-1",
      "ap-northeast-1"
    ]
  }
}
```

Create and inspect a testbed:

```bash
cd benchmark
fab create --nodes=10
fab info
```

Install the configured branch and run the remote benchmark:

```bash
fab install
fab remote
```

Inspect logs and generate figures:

```bash
fab logs
fab plot
```

Stop or destroy the testbed when finished:

```bash
fab kill
fab stop
fab destroy
```

## License

This software is licensed under the [Apache License 2.0](LICENSE).
