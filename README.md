# MRV

[Supplementary material](Pengkun_Ren_ICDE2027_Supplementary.pdf) · [Code](code/) · [Experimental artifacts](code/output/INDEX.md)

MRV is a research prototype that adds a post-consensus structural ordering
layer to a Narwhal/Tusk-style DAG-BFT stack. It is based on the open-source
[Narwhal and Tusk](https://github.com/asonnino/narwhal) implementation.

MRV preserves worker dissemination and the Tusk commit rule.
After Tusk emits a committed sub-DAG, MRV uses authenticated creator and round metadata together with ancestry reconstructed from authenticated parent references to derive a deterministic order within that execution slice. Slice membership and the order between slices remain fixed by Tusk.

The primary also checks the GC boundary before voting, including on resumed paths.

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
├── Pengkun_Ren_ICDE2027_Supplementary.pdf
├── code/
│   ├── benchmark/       # AWS and benchmark harnesses
│   ├── config/          # Committee and protocol configuration
│   ├── consensus/       # Tusk commitment and ancestry-complete export
│   ├── crypto/          # Cryptographic primitives
│   ├── mrv_executor/    # MRV post-consensus ordering layer
│   ├── network/         # Networking utilities
│   ├── node/            # Node binary and pipeline wiring
│   ├── primary/         # Narwhal primary and voting safeguards
│   ├── store/           # RocksDB-backed storage
│   ├── worker/          # Narwhal worker
│   └── output/          # Selected experimental artifacts
├── README.md
└── LICENSE
```

The protocol implementation is written in Rust. Benchmark orchestration is
written in Python and uses Fabric.

## Requirements

- A Rust toolchain
- Python 3.9+
- Clang, required when building RocksDB
- `tmux`, used by the benchmark harnesses
- AWS credentials, only for remote experiments

## AWS benchmarks

Before creating a testbed, edit `code/benchmark/settings.json`. For example:

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
cd code/benchmark
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
