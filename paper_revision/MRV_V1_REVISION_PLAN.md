# MRV v1 revision plan

This plan separates the revision into three priorities. The priorities describe submission urgency, not independent workstreams. The technical definitions must be fixed before the narrative sections are rewritten.

## P0 — complete now

| ID | Task | Paper location | Code impact | Acceptance condition |
|---|---|---|---|---|
| P0.1 | Freeze the Protocol Decision Log | Sections 3–4 | None initially | Every symbol, window, snapshot, graph edge, and output object has one definition |
| P0.2 | Define the Exporter Contract | Section 3.1 | Adapter audit later | Common ordered slices, exact-once committed-vertex membership, immutable prefixes, canonical creator-round output, and causal slice delivery are explicit |
| P0.3 | Build the Paper–Spec–Code Matrix | Revision artifact | None | Every protocol claim is marked as implemented, missing, or text-only |
| P0.4 | Replace endogenous stopping with a fixed window | Sections 3.4–3.5, 4.2–4.3, algorithms, proofs | Required later | First threshold crossing records eligibility but never closes a pair window |
| P0.5 | Add hard causal edges | Section 4 graph construction | Required later | Causally comparable pairs never rely on the structural comparator |
| P0.6 | Define deterministic SCC linearization | Section 4 linearization | Required later | SCC ties use the minimum member key, and each SCC is refined by a deterministic topological sort of its causal subgraph |
| P0.7 | Rewrite the technical model | Entire Section 3 | None | The model uses commit-prefix snapshots rather than a highest-round committed view |
| P0.8 | Rewrite the MRV protocol | Entire Section 4 | Required later | The protocol matches the frozen definitions and uses one slice-seal snapshot |
| P0.9 | Replace the main guarantees | Analysis subsection | None | Four main theorems and four supporting results use the new objects |

### P0 editing order

1. Section 3: System Model and Problem Statement.
2. Section 4: fixed-window evidence extraction and pair comparator.
3. Section 4: causal graph, ordered SCCs, and execution refinement.
4. Analysis: definitions, theorem statements, and proof sketches.
5. Paper–Spec–Code Matrix review.
6. Required code alignment and tests.

Section 3 must precede Section 4. In particular, the protocol cannot be written clearly until the exporter, snapshot, eligibility, and output contracts are fixed.

## P1 — complete in this paper revision

| ID | Task | Paper location | Depends on |
|---|---|---|---|
| P1.1 | Rewrite the Threat Model | Section 3.2 | Exporter Contract |
| P1.2 | Define committed-vertex/transaction projection | Section 3.3 and analysis | Exact-once slice membership |
| P1.3 | Replace the fairness claim | Section 3.5 and all later claims | Fixed ordering-edge definition |
| P1.4 | Rewrite the Security Discussion | Section 4.6 | Threshold proof and adversarial influence |
| P1.5 | Rewrite Limitations | Section 4.6 | Final claim boundary |
| P1.6 | Update Related Work | Background/Related Work section | Final positioning |
| P1.7 | Define new metrics | Evaluation setup | Final graph and output semantics |
| P1.8 | Remove unsupported storage claims | Complexity and implementation | Paper–Spec–Code Matrix |

Although the Threat Model is P1, it is drafted together with Section 3 because it defines what the P0 guarantees mean.

The required new metrics are:

- eligible and ineligible vertex rates;
- Edge, Conflict, NoSignal, and Ineligible pair rates;
- ordering-edge coverage;
- SCC count and size;
- inter-SCC and intra-SCC ordering-edge rates;
- fraction of ordering choices resolved by the completion key;
- slice sealing delay under the fixed window.

The metrics are defined during the technical rewrite. Their final values are added after code and experiment updates.

## P2 — finalize after protocol and measurements stabilize

| ID | Task | Paper location | Trigger |
|---|---|---|---|
| P2.1 | Rewrite Abstract | Abstract | P0 and P1 claims frozen |
| P2.2 | Rewrite Introduction | Section 1 | Protocol and security story frozen |
| P2.3 | Rewrite Contributions | Section 1 | Novelty statement frozen |
| P2.4 | Rewrite Conclusion | Final section | Results and limitations frozen |
| P2.5 | Finalize evaluation claims | Evaluation | New measurements available |
| P2.6 | Decide on safe store GC | Implementation/limitations | Before submission |
| P2.7 | Decide on randomized completion | Protocol/limitations | Completion-key coverage known |

Existing performance measurements may be retained only where the corresponding protocol behavior is unchanged. Fixed-window latency, graph coverage, SCC structure, and completion-key dependence require new measurements.

## Section-to-task map

| Paper section | Required revision | Priority |
|---|---|---|
| Section 2: Background/Related Work | Separate standard transaction order fairness from MRV structural evidence; add recent DAG ordering work | P1 |
| Section 3.1: System and Exporter Model | Replicas, authenticated DAG metadata, common prefixes, exact-once slices, and causal delivery | P0 |
| Section 3.2: Threat Model | Scheduling and parent-selection influence; authentication binds creator and parents but not arrival order | P1, drafted now |
| Section 3.3: Committed Vertices and Transactions | Vertex scope, transaction uniqueness, within-vertex and cross-slice limits | P1, drafted now |
| Section 3.4: Committed Snapshot and Visibility | \(P_k\), \(\rho(k)\), \(k_S^\star\), \(C_X^{(k)}(t)\), and fixed eligibility | P0 |
| Section 3.5: Pairwise Ordering Edges and SCC Order | Fixed pair window, ordering edges, ordered SCCs, and edge preservation | P0 |
| Section 3.6: Problem Statement | Inputs, ordered SCCs, execution order, and global causality | P0 |
| Section 4.1: Overview | Fixed-window, one-snapshot slice lifecycle | P0 |
| Section 4.2: Snapshot Evidence Extraction | Per-vertex fixed eligibility window and snapshot-only evidence | P0 |
| Section 4.3: Pairwise Comparison | Post-coexistence fixed signal window and four outcomes | P0 |
| Section 4.4: Graph Construction and Linearization | Hard causal edges, ordering edges, SCC tie-breaking, and causal refinement | P0 |
| Section 4.5: Replica Procedure and Cost | One-shot slice sealing, ordered release, auxiliary-state and cost bounds | P0/P1 |
| Section 4.6: Security Scope and Limitations | Exact threshold meaning and adversarial control of evidence | P1 |
| Section 4.7: Analysis and Guarantees | Four main theorems and four supporting results | P0 |
| Evaluation | New ordering-edge, SCC, completion-key, and seal-delay metrics | P1/P2 |
| Abstract/Introduction/Conclusion | Rewrite last | P2 |

## Initial Paper–Spec–Code Matrix

| Property | MRV v1 specification | Current paper | Current implementation | Action |
|---|---|---|---|---|
| Slice membership | Exact once, common ordered sequence | Described informally | `CommittedSubDag` has ordered `batch_index`; duplicate filtering needs an explicit audit | Formalize and test |
| Cross-slice causality | Ancestor is in the same or an earlier slice | Not a formal exporter property | Expected from committed causal-history delivery | Formalize and test |
| Snapshot ancestry | Every prefix contains the parent metadata needed for ancestry queries | Not explicit | Expected from committed causal-history storage | Verify and document |
| Evidence snapshot | One immutable prefix \(P_{k_S^\star}\) per slice | Highest committed round is used as the view index | Computation occurs when a batch finalizes | Rewrite model and verify equivalence |
| Eligibility | Threshold reached within \([r(X),r(X)+W]\) | First crossing also closes the horizon | First crossing sets `horizon` | Change protocol and code later |
| Pair horizon | \(s(A,B)+W\) | \(\max(h_A,h_B)\) | \(\max(h_A,h_B)\) | Change protocol and code later |
| Verdict computation | Once at the slice snapshot | Claims per-pair freezing | Recomputed when sorting the batch | Rewrite paper; align state semantics |
| Causal graph | Explicit hard causal edges | Claimed | Missing from graph construction | Add later |
| SCC tie-breaking | Minimum member \(\kappa\)-key among zero-indegree SCCs | Not fully specified | Implemented by `compare_components` | Retain and document |
| SCC refinement | Topological sort of causal subgraph, then deterministic key | Claimed | Global round-first key is used inside SCCs | Implement explicitly or prove exact equivalence |
| Completion key | No fairness guarantee | Wording is too favorable | `(round, creator, digest)` | Narrow claim and add metrics |
| Storage bound | Active auxiliary state only | Claims all state is released | Committed metadata store is retained | Remove end-to-end bound; revisit GC before submission |
