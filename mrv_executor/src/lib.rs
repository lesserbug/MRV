use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
#[cfg(feature = "benchmark")]
use std::convert::TryInto;
#[cfg(feature = "benchmark")]
use std::time::Instant;

use consensus::CommittedSubDag;
use crypto::Hash as _;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::error;
use log::{debug, info, warn};
use primary::Certificate;
use tokio::sync::mpsc::{Receiver, Sender};

pub type Round = u64;
pub type Digest = crypto::Digest;

#[derive(Clone)]
struct AufState {
    round: Round,
    seen_by_round: BTreeMap<Round, HashSet<PublicKey>>,
}

impl AufState {
    fn new(round: Round) -> Self {
        Self {
            round,
            seen_by_round: BTreeMap::new(),
        }
    }
}

struct SliceState {
    // This is the exporter/Tusk execution order before MRV intervenes.
    members: Vec<Digest>,
    max_round: Round,
    seal_horizon: Round,
    sealed: Option<SealedSlice>,
    #[cfg(feature = "benchmark")]
    registered_at: Instant,
}

struct SealedSlice {
    // Both outputs are fixed at sealing. Only these final outputs survive after
    // the slice-local evidence and temporary graph are reclaimed.
    ordered_sccs: Vec<Vec<Digest>>,
    order: Vec<Digest>,
    #[cfg(feature = "benchmark")]
    metrics: SealedMetrics,
}

struct SliceOrdering {
    ordered_sccs: Vec<Vec<Digest>>,
    order: Vec<Digest>,
    #[cfg(feature = "benchmark")]
    metrics: OrderingMetrics,
}

struct Linearization {
    ordered_sccs: Vec<Vec<Digest>>,
    order: Vec<Digest>,
    #[cfg(feature = "benchmark")]
    node_to_component: HashMap<Digest, usize>,
    #[cfg(feature = "benchmark")]
    component_graph: HashMap<usize, HashSet<usize>>,
}

#[cfg(feature = "benchmark")]
#[derive(Default)]
struct OrderingMetrics {
    eligible_vertex_count: usize,
    ineligible_vertex_count: usize,
    all_pair_count: usize,
    causal_pair_count: usize,
    incomparable_pair_count: usize,
    eligible_pair_count: usize,
    ineligible_pair_count: usize,
    edge_count: usize,
    conflict_count: usize,
    no_signal_count: usize,
    intra_scc_ordering_edge_count: usize,
    inter_scc_ordering_edge_count: usize,
    constrained_pair_count: usize,
    scc_count: usize,
    nontrivial_scc_count: usize,
    vertices_in_nontrivial_sccs: usize,
    max_scc_size: usize,
    incomparable_pair_inversion_count: usize,
    constrained_inversion_count: usize,
    unconstrained_inversion_count: usize,
    moved_vertex_count: usize,
    unchanged_slice: bool,
    position_displacement_median: f64,
    position_displacement_p95: f64,
}

#[cfg(feature = "benchmark")]
struct SealedMetrics {
    ordering: OrderingMetrics,
    snapshot_round: Round,
    trigger_slice_id: u64,
    trigger_export_prefix_digest: Digest,
    execution_order_digest: Digest,
    seal_delay_ms: u128,
}

#[derive(Debug, Eq, PartialEq)]
enum PairVerdict {
    EdgeAToB,
    EdgeBToA,
    Ineligible,
    Conflict,
    NoSignal,
}

pub struct MrvExecutor {
    rx_input: Receiver<CommittedSubDag>,
    tx_output: Sender<Certificate>,

    // Bounded cache for active ancestry queries and sealed outputs awaiting
    // release. Historical certificates below the active floor are reclaimed.
    store: HashMap<Digest, Certificate>,

    // Evidence exists only for vertices in unsealed slices. A sealed slice
    // remains in `slices` only as its final output until exporter-order release.
    auf_states: HashMap<Digest, AufState>,
    slices: BTreeMap<u64, SliceState>,
    active_floor_round: Option<Round>,

    // Highest round in the currently processed committed prefix.
    frontier_round: Round,

    // MRV system parameters.
    mrv_window: Round,
    reach_threshold: usize, // q_vis = 2f + 1
    delta_threshold: i64,   // theta = f + 1

    #[cfg(feature = "benchmark")]
    seen_slice_members: HashMap<Digest, u64>,
    #[cfg(feature = "benchmark")]
    cumulative_member_count: u64,
    #[cfg(feature = "benchmark")]
    export_prefix_digest: Digest,
}

impl MrvExecutor {
    pub fn spawn(
        rx_input: Receiver<CommittedSubDag>,
        tx_output: Sender<Certificate>,
        committee_size: usize,
        mrv_window: Round,
    ) {
        let n = committee_size.max(1);
        let f = n.saturating_sub(1) / 3;
        let reach_threshold = 2 * f + 1;
        let delta_threshold = (f + 1) as i64;
        assert!(mrv_window > 0, "MRV window must be positive");

        tokio::spawn(async move {
            info!(
                "MRV Executor started: n={}, f={}, mrv_window={}, reach={}, delta={}",
                n, f, mrv_window, reach_threshold, delta_threshold
            );

            let mut executor = Self {
                rx_input,
                tx_output,
                store: HashMap::new(),
                auf_states: HashMap::new(),
                slices: BTreeMap::new(),
                active_floor_round: None,
                frontier_round: 0,
                mrv_window,
                reach_threshold,
                delta_threshold,
                #[cfg(feature = "benchmark")]
                seen_slice_members: HashMap::new(),
                #[cfg(feature = "benchmark")]
                cumulative_member_count: 0,
                #[cfg(feature = "benchmark")]
                export_prefix_digest: Digest::default(),
            };
            executor.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some(committed_slice) = self.rx_input.recv().await {
            if self.on_committed_slice(committed_slice).await.is_err() {
                warn!("MRV stopped because downstream receiver was dropped");
                return;
            }
        }
    }

    async fn on_committed_slice(&mut self, committed_slice: CommittedSubDag) -> Result<(), ()> {
        #[cfg(feature = "benchmark")]
        let registered_at = Instant::now();

        debug!(
            "Registering execution slice id={} leader_round={} leader_digest={:?} size={}",
            committed_slice.batch_index,
            committed_slice.leader_round,
            committed_slice.leader_digest,
            committed_slice.certificates.len()
        );

        let slice_id = committed_slice.batch_index;
        #[cfg(feature = "benchmark")]
        let leader_round = committed_slice.leader_round;
        #[cfg(feature = "benchmark")]
        let leader_digest = committed_slice.leader_digest;
        let mut certificates = committed_slice.certificates;
        let members: Vec<Digest> = certificates
            .iter()
            .map(|certificate| certificate.digest())
            .collect();
        let max_round = certificates
            .iter()
            .map(Certificate::round)
            .max()
            .unwrap_or(0);

        if members.is_empty() {
            return Ok(());
        }

        #[cfg(feature = "benchmark")]
        for certificate in &certificates {
            let member = certificate.digest();
            if let Some(first_slice_id) = self.seen_slice_members.get(&member) {
                error!(
                    "MRV_ExactOnceViolation first_slice_id={} duplicate_slice_id={} member_digest={:?} member_round={} member_creator={:?}",
                    first_slice_id,
                    slice_id,
                    member,
                    certificate.round(),
                    certificate.origin(),
                );
                panic!("MRV exact-once membership invariant violated");
            }
            self.seen_slice_members.insert(member, slice_id);
        }

        #[cfg(feature = "benchmark")]
        let (member_set_digest, member_order_digest, export_prefix_digest) = {
            let member_order_digest = fingerprint_digests(b"MRV-MEMBER-ORDER-v1", &members);
            let mut sorted_members = members.clone();
            sorted_members.sort();
            let member_set_digest = fingerprint_digests(b"MRV-MEMBER-SET-v1", &sorted_members);

            self.cumulative_member_count += members.len() as u64;
            let mut hasher = Sha512::new();
            hasher.update(b"MRV-EXPORT-PREFIX-v1");
            hasher.update(self.export_prefix_digest.as_ref());
            hasher.update(leader_round.to_le_bytes());
            hasher.update(leader_digest.as_ref());
            hasher.update(member_set_digest.as_ref());
            let export_prefix_digest = crypto::Digest(
                hasher.finalize()[..32]
                    .try_into()
                    .expect("SHA-512 output must contain 32 bytes"),
            );
            self.export_prefix_digest = export_prefix_digest.clone();

            (member_set_digest, member_order_digest, export_prefix_digest)
        };

        // A CommittedSubDag is one immutable exporter event. Process the whole
        // event before sealing so every decision uses exactly one prefix P_k.
        // Evidence processing follows nondecreasing rounds, while `members`
        // retains the exporter sequence unchanged as the base execution order.
        certificates.sort_by_key(Certificate::round);
        for certificate in certificates {
            let digest = certificate.digest();
            let round = certificate.round();
            let author = certificate.origin();

            self.frontier_round = self.frontier_round.max(round);
            self.store.insert(digest.clone(), certificate);
            if !self.auf_states.contains_key(&digest) {
                self.note_active_round(round);
                self.auf_states.insert(digest.clone(), AufState::new(round));
            }
            self.update_seen_for_new_certificate(round, author, &digest);
        }

        let seal_horizon = max_round.saturating_add(self.mrv_window);
        #[cfg(feature = "benchmark")]
        info!(
            "MRV_SliceRegistered slice_id={} leader_round={} leader_digest={:?} slice_size={} seal_horizon={} member_set_digest={:?} member_order_digest={:?} cumulative_member_count={} export_prefix_digest={:?}",
            slice_id,
            leader_round,
            leader_digest,
            members.len(),
            seal_horizon,
            member_set_digest,
            member_order_digest,
            self.cumulative_member_count,
            export_prefix_digest,
        );
        let previous = self.slices.insert(
            slice_id,
            SliceState {
                members,
                max_round,
                seal_horizon,
                sealed: None,
                #[cfg(feature = "benchmark")]
                registered_at,
            },
        );
        debug_assert!(previous.is_none(), "slice ids must be unique");

        let sealed_any = self.seal_ready_slices(slice_id);
        self.release_ready_slices().await?;
        if sealed_any {
            self.garbage_collect_store();
            #[cfg(feature = "benchmark")]
            self.assert_member_lifecycle();
        }
        Ok(())
    }

    fn update_seen_for_new_certificate(
        &mut self,
        round: Round,
        author: PublicKey,
        digest: &Digest,
    ) {
        let min_round = self.active_floor_round.unwrap_or(round);
        let mut stack = vec![digest.clone()];
        let mut visited = HashSet::new();

        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }

            let current_round = match self.store.get(&current) {
                Some(certificate) => certificate.round(),
                None => continue,
            };

            if current_round < min_round {
                continue;
            }

            if let Some(state) = self.auf_states.get_mut(&current) {
                // Multiple ancestry paths still contribute at most once for a
                // creator-round because the value is a creator set.
                state.seen_by_round.entry(round).or_default().insert(author);
            }

            if current_round == min_round {
                continue;
            }

            if let Some(certificate) = self.store.get(&current) {
                for parent in &certificate.header.parents {
                    stack.push(parent.clone());
                }
            }
        }
    }

    fn note_active_round(&mut self, round: Round) {
        self.active_floor_round = Some(
            self.active_floor_round
                .map_or(round, |current_min| current_min.min(round)),
        );
    }

    fn recompute_active_floor_round(&mut self) {
        self.active_floor_round = self.auf_states.values().map(|state| state.round).min();
    }

    fn seal_ready_slices(&mut self, trigger_slice_id: u64) -> bool {
        #[cfg(not(feature = "benchmark"))]
        let _ = trigger_slice_id;
        let ready: Vec<u64> = self
            .slices
            .iter()
            .filter_map(|(&slice_id, slice)| {
                (slice.sealed.is_none() && self.frontier_round >= slice.seal_horizon)
                    .then_some(slice_id)
            })
            .collect();
        let sealed_any = !ready.is_empty();

        for slice_id in ready {
            let members = self
                .slices
                .get(&slice_id)
                .expect("ready slice must exist")
                .members
                .clone();

            let ordering = self.order_slice(&members);
            #[cfg(feature = "benchmark")]
            let execution_order_digest =
                fingerprint_digests(b"MRV-EXECUTION-ORDER-v1", &ordering.order);
            let sealed = SealedSlice {
                ordered_sccs: ordering.ordered_sccs,
                order: ordering.order,
                #[cfg(feature = "benchmark")]
                metrics: SealedMetrics {
                    ordering: ordering.metrics,
                    snapshot_round: self.frontier_round,
                    trigger_slice_id,
                    trigger_export_prefix_digest: self.export_prefix_digest.clone(),
                    execution_order_digest,
                    seal_delay_ms: self
                        .slices
                        .get(&slice_id)
                        .expect("ready slice must exist")
                        .registered_at
                        .elapsed()
                        .as_millis(),
                },
            };

            let slice = self
                .slices
                .get_mut(&slice_id)
                .expect("ready slice must exist");
            slice.sealed = Some(sealed);
            slice.members = Vec::new();

            // Later prefixes must not update or reconstruct evidence for a
            // sealed slice. Its final SCC and execution orders are sufficient.
            self.release_evidence(&members);
            #[cfg(feature = "benchmark")]
            self.assert_member_lifecycle();

            #[cfg(feature = "benchmark")]
            {
                let slice = self.slices.get(&slice_id).expect("sealed slice must exist");
                let metrics = &slice
                    .sealed
                    .as_ref()
                    .expect("sealed slice must contain final metrics")
                    .metrics;
                // Emit structural results at sealing so a later exporter-order
                // blockage cannot hide an otherwise complete sample.
                self.log_slice_metrics(slice_id, slice, metrics);
            }
        }
        sealed_any
    }

    async fn release_ready_slices(&mut self) -> Result<(), ()> {
        loop {
            let slice_id = match self.slices.iter().next() {
                Some((&slice_id, slice)) if slice.sealed.is_some() => slice_id,
                Some(_) | None => return Ok(()),
            };

            let mut slice = self
                .slices
                .remove(&slice_id)
                .expect("release-ready slice must exist");
            let sealed = slice
                .sealed
                .take()
                .expect("release-ready slice must be sealed");

            debug!(
                "Releasing execution slice id={} sccs={} vertices={} max_round={} seal_horizon={}",
                slice_id,
                sealed.ordered_sccs.len(),
                sealed.order.len(),
                slice.max_round,
                slice.seal_horizon,
            );

            for digest in &sealed.order {
                let certificate = self
                    .store
                    .get(digest)
                    .cloned()
                    .expect("sealed slice member must remain in the committed store");
                self.tx_output.send(certificate).await.map_err(|_| ())?;
            }

            #[cfg(feature = "benchmark")]
            info!(
                "MRV_SliceRelease slice_id={} release_delay_ms={}",
                slice_id,
                slice.registered_at.elapsed().as_millis(),
            );
        }
    }

    fn release_evidence(&mut self, members: &[Digest]) {
        let mut removed_floor = false;
        for digest in members {
            if self
                .auf_states
                .get(digest)
                .map_or(false, |state| Some(state.round) == self.active_floor_round)
            {
                removed_floor = true;
            }
            self.auf_states.remove(digest);
        }
        if removed_floor {
            self.recompute_active_floor_round();
        }
    }

    fn garbage_collect_store(&mut self) {
        let queued_outputs: HashSet<Digest> = self
            .slices
            .values()
            .filter_map(|slice| slice.sealed.as_ref())
            .flat_map(|sealed| sealed.order.iter().cloned())
            .collect();
        let active_floor = self.active_floor_round;

        self.store.retain(|digest, certificate| {
            queued_outputs.contains(digest)
                || active_floor.map_or(false, |floor| certificate.round() >= floor)
        });
    }

    #[cfg(feature = "benchmark")]
    fn assert_member_lifecycle(&self) {
        for (&slice_id, slice) in &self.slices {
            if let Some(sealed) = &slice.sealed {
                for member in &sealed.order {
                    if !self.store.contains_key(member) {
                        error!(
                            "MRV_MemberLifecycleViolation reason=missing_queued_output_store slice_id={} member_digest={:?}",
                            slice_id, member
                        );
                        panic!("MRV queued output member is missing from the committed store");
                    }
                }
            } else {
                for member in &slice.members {
                    if !self.store.contains_key(member) {
                        error!(
                            "MRV_MemberLifecycleViolation reason=missing_unsealed_store slice_id={} member_digest={:?}",
                            slice_id, member
                        );
                        panic!("MRV unsealed member is missing from the committed store");
                    }
                    if !self.auf_states.contains_key(member) {
                        error!(
                            "MRV_MemberLifecycleViolation reason=missing_unsealed_auf_state slice_id={} member_digest={:?}",
                            slice_id, member
                        );
                        panic!("MRV unsealed member is missing visibility state");
                    }
                }
            }
        }
    }

    fn order_slice(&self, members: &[Digest]) -> SliceOrdering {
        let mut nodes = members.to_vec();
        nodes.sort_by(|a, b| self.tie_break(a, b));

        let eligible: HashMap<Digest, bool> = nodes
            .iter()
            .cloned()
            .map(|digest| {
                let is_eligible = self.is_eligible(&digest);
                (digest, is_eligible)
            })
            .collect();

        let mut graph: HashMap<Digest, HashSet<Digest>> =
            nodes.iter().cloned().map(|d| (d, HashSet::new())).collect();
        let mut causal_edges = HashSet::new();

        #[cfg(feature = "benchmark")]
        let mut ordering_edges = Vec::new();
        #[cfg(feature = "benchmark")]
        let mut incomparable_pairs = Vec::new();
        #[cfg(feature = "benchmark")]
        let mut metrics = OrderingMetrics {
            eligible_vertex_count: eligible.values().filter(|value| **value).count(),
            ineligible_vertex_count: eligible.values().filter(|value| !**value).count(),
            all_pair_count: nodes.len().saturating_mul(nodes.len().saturating_sub(1)) / 2,
            ..OrderingMetrics::default()
        };

        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let a = &nodes[i];
                let b = &nodes[j];

                if let Some((from, to)) = self.causal_edge(a, b) {
                    graph
                        .get_mut(&from)
                        .expect("causal endpoint must be in slice")
                        .insert(to.clone());
                    causal_edges.insert((from, to));
                    #[cfg(feature = "benchmark")]
                    {
                        metrics.causal_pair_count += 1;
                    }
                    continue;
                }

                let eligible_a = *eligible.get(a).expect("slice vertex must have eligibility");
                let eligible_b = *eligible.get(b).expect("slice vertex must have eligibility");
                let verdict = self.compare_incomparable_pair(a, b, eligible_a, eligible_b);

                #[cfg(feature = "benchmark")]
                {
                    metrics.incomparable_pair_count += 1;
                    incomparable_pairs.push((a.clone(), b.clone()));
                    if eligible_a && eligible_b {
                        metrics.eligible_pair_count += 1;
                    }
                }

                match verdict {
                    PairVerdict::EdgeAToB => {
                        graph
                            .get_mut(a)
                            .expect("ordering endpoint must be in slice")
                            .insert(b.clone());
                        #[cfg(feature = "benchmark")]
                        {
                            metrics.edge_count += 1;
                            ordering_edges.push((a.clone(), b.clone()));
                        }
                    }
                    PairVerdict::EdgeBToA => {
                        graph
                            .get_mut(b)
                            .expect("ordering endpoint must be in slice")
                            .insert(a.clone());
                        #[cfg(feature = "benchmark")]
                        {
                            metrics.edge_count += 1;
                            ordering_edges.push((b.clone(), a.clone()));
                        }
                    }
                    PairVerdict::Ineligible => {
                        #[cfg(feature = "benchmark")]
                        {
                            metrics.ineligible_pair_count += 1;
                        }
                    }
                    PairVerdict::Conflict => {
                        #[cfg(feature = "benchmark")]
                        {
                            metrics.conflict_count += 1;
                        }
                    }
                    PairVerdict::NoSignal => {
                        #[cfg(feature = "benchmark")]
                        {
                            metrics.no_signal_count += 1;
                        }
                    }
                }
            }
        }

        let linearization = self.linearize_graph(&nodes, &graph, &causal_edges);

        #[cfg(feature = "benchmark")]
        {
            for (from, to) in &ordering_edges {
                if linearization.node_to_component[from] == linearization.node_to_component[to] {
                    metrics.intra_scc_ordering_edge_count += 1;
                } else {
                    metrics.inter_scc_ordering_edge_count += 1;
                }
            }

            metrics.scc_count = linearization.ordered_sccs.len();
            metrics.nontrivial_scc_count = linearization
                .ordered_sccs
                .iter()
                .filter(|component| component.len() > 1)
                .count();
            metrics.vertices_in_nontrivial_sccs = linearization
                .ordered_sccs
                .iter()
                .filter(|component| component.len() > 1)
                .map(Vec::len)
                .sum();
            metrics.max_scc_size = linearization
                .ordered_sccs
                .iter()
                .map(Vec::len)
                .max()
                .unwrap_or(0);

            // Reachability is computed once per SCC. Re-running a graph walk
            // for every vertex pair would make benchmark instrumentation
            // dominate the ordering path on large slices.
            let component_reachability = Self::component_reachability(
                linearization.ordered_sccs.len(),
                &linearization.component_graph,
            );
            metrics.constrained_pair_count = Self::count_constrained_pairs(
                &incomparable_pairs,
                &linearization.node_to_component,
                &component_reachability,
            );

            self.collect_intervention_metrics(
                members,
                &linearization.order,
                &incomparable_pairs,
                &linearization.node_to_component,
                &component_reachability,
                &mut metrics,
            );

            debug_assert_eq!(
                metrics.all_pair_count,
                metrics.causal_pair_count + metrics.incomparable_pair_count
            );
            debug_assert_eq!(
                metrics.incomparable_pair_count,
                metrics.ineligible_pair_count
                    + metrics.edge_count
                    + metrics.conflict_count
                    + metrics.no_signal_count
            );
            debug_assert_eq!(
                metrics.eligible_pair_count,
                metrics.edge_count + metrics.conflict_count + metrics.no_signal_count
            );
            debug_assert_eq!(
                metrics.edge_count,
                metrics.intra_scc_ordering_edge_count + metrics.inter_scc_ordering_edge_count
            );
            debug_assert_eq!(
                metrics.incomparable_pair_inversion_count,
                metrics.constrained_inversion_count + metrics.unconstrained_inversion_count
            );
        }

        SliceOrdering {
            ordered_sccs: linearization.ordered_sccs,
            order: linearization.order,
            #[cfg(feature = "benchmark")]
            metrics,
        }
    }

    fn is_eligible(&self, digest: &Digest) -> bool {
        let state = match self.auf_states.get(digest) {
            Some(state) => state,
            None => return false,
        };
        let end = state.round.saturating_add(self.mrv_window);

        // Eligibility is evaluated once at the common seal snapshot over the
        // full per-vertex interval. A crossing never shortens pair evidence.
        state
            .seen_by_round
            .range(state.round..=end)
            .any(|(_, creators)| creators.len() >= self.reach_threshold)
    }

    fn compare_incomparable_pair(
        &self,
        a: &Digest,
        b: &Digest,
        eligible_a: bool,
        eligible_b: bool,
    ) -> PairVerdict {
        if !eligible_a || !eligible_b {
            return PairVerdict::Ineligible;
        }

        let state_a = match self.auf_states.get(a) {
            Some(state) => state,
            None => return PairVerdict::Ineligible,
        };
        let state_b = match self.auf_states.get(b) {
            Some(state) => state,
            None => return PairVerdict::Ineligible,
        };

        let coexistence_round = state_a.round.max(state_b.round);
        let first_round = coexistence_round.saturating_add(1);
        let last_round = coexistence_round.saturating_add(self.mrv_window);
        let mut pos_ab = false;
        let mut pos_ba = false;

        // Always inspect the complete post-coexistence window. Crossings are
        // binary evidence; repeated crossings add no weight.
        for round in first_round..=last_round {
            let delta = self.seen_count_at(a, round) as i64 - self.seen_count_at(b, round) as i64;
            pos_ab |= delta >= self.delta_threshold;
            pos_ba |= delta <= -self.delta_threshold;
        }

        match (pos_ab, pos_ba) {
            (true, false) => PairVerdict::EdgeAToB,
            (false, true) => PairVerdict::EdgeBToA,
            (true, true) => PairVerdict::Conflict,
            (false, false) => PairVerdict::NoSignal,
        }
    }

    fn seen_count_at(&self, digest: &Digest, round: Round) -> usize {
        self.auf_states
            .get(digest)
            .and_then(|state| state.seen_by_round.get(&round))
            .map_or(0, HashSet::len)
    }

    fn causal_edge(&self, a: &Digest, b: &Digest) -> Option<(Digest, Digest)> {
        let round_a = self
            .store
            .get(a)
            .expect("left slice member certificate missing from MRV store during causal comparison")
            .round();
        let round_b = self
            .store
            .get(b)
            .expect(
                "right slice member certificate missing from MRV store during causal comparison",
            )
            .round();

        // Parents always have lower rounds, so equal-round vertices are
        // incomparable and only the older endpoint can be an ancestor.
        match round_a.cmp(&round_b) {
            Ordering::Equal => None,
            Ordering::Less => self
                .is_ancestor(a, b, round_a)
                .then(|| (a.clone(), b.clone())),
            Ordering::Greater => self
                .is_ancestor(b, a, round_b)
                .then(|| (b.clone(), a.clone())),
        }
    }

    fn is_ancestor(&self, ancestor: &Digest, descendant: &Digest, ancestor_round: Round) -> bool {
        let mut stack = vec![descendant.clone()];
        let mut visited = HashSet::new();

        while let Some(current) = stack.pop() {
            if current == *ancestor {
                return true;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(certificate) = self.store.get(&current) {
                if certificate.round() > ancestor_round {
                    stack.extend(certificate.header.parents.iter().cloned());
                }
            }
        }
        false
    }

    fn linearize_graph(
        &self,
        nodes: &[Digest],
        graph: &HashMap<Digest, HashSet<Digest>>,
        causal_edges: &HashSet<(Digest, Digest)>,
    ) -> Linearization {
        let components = self.find_scc(nodes, graph);

        let mut keyed_nodes = nodes.to_vec();
        keyed_nodes.sort_by(|a, b| self.tie_break(a, b));
        let node_rank: HashMap<Digest, usize> = keyed_nodes
            .into_iter()
            .enumerate()
            .map(|(rank, digest)| (digest, rank))
            .collect();

        let mut node_to_component = HashMap::new();
        for (component_idx, component) in components.iter().enumerate() {
            for digest in component {
                node_to_component.insert(digest.clone(), component_idx);
            }
        }

        let mut component_graph: HashMap<usize, HashSet<usize>> =
            (0..components.len()).map(|i| (i, HashSet::new())).collect();
        let mut component_indegree: HashMap<usize, usize> =
            (0..components.len()).map(|i| (i, 0)).collect();

        for (from, tos) in graph {
            let from_component = node_to_component[from];
            for to in tos {
                let to_component = node_to_component[to];
                if from_component != to_component
                    && component_graph
                        .get_mut(&from_component)
                        .expect("component must exist")
                        .insert(to_component)
                {
                    *component_indegree
                        .get_mut(&to_component)
                        .expect("component must exist") += 1;
                }
            }
        }

        // Build the SCC-local hard-causal adjacency once. Scanning the full
        // causal-edge set separately for every SCC would make linearization
        // superlinear in the number of components.
        let mut causal_outgoing: HashMap<Digest, Vec<Digest>> = nodes
            .iter()
            .cloned()
            .map(|digest| (digest, Vec::new()))
            .collect();
        for (from, to) in causal_edges {
            if node_to_component[from] == node_to_component[to] {
                causal_outgoing
                    .get_mut(from)
                    .expect("causal endpoint must be in slice")
                    .push(to.clone());
            }
        }

        let component_rank: Vec<usize> = components
            .iter()
            .map(|component| {
                component
                    .iter()
                    .map(|digest| node_rank[digest])
                    .min()
                    .expect("component should not be empty")
            })
            .collect();
        let mut ready = BinaryHeap::new();
        for (&component_idx, &degree) in &component_indegree {
            if degree == 0 {
                ready.push(Reverse((component_rank[component_idx], component_idx)));
            }
        }
        let mut ordered_sccs = Vec::with_capacity(components.len());
        let mut order = Vec::with_capacity(nodes.len());

        while let Some(Reverse((_, current))) = ready.pop() {
            // The combined SCC may be cyclic, but its hard causal subgraph is
            // a DAG and must be the sole constraint used for internal order.
            let component_order = self.topological_sort_causal_component(
                &components[current],
                &causal_outgoing,
                &node_rank,
            );
            order.extend(component_order.iter().cloned());
            ordered_sccs.push(component_order);

            for &next in component_graph.get(&current).expect("component must exist") {
                let degree = component_indegree
                    .get_mut(&next)
                    .expect("component must exist");
                *degree -= 1;
                if *degree == 0 {
                    ready.push(Reverse((component_rank[next], next)));
                }
            }
        }

        assert_eq!(
            order.len(),
            nodes.len(),
            "condensation graph must be acyclic"
        );

        Linearization {
            ordered_sccs,
            order,
            #[cfg(feature = "benchmark")]
            node_to_component,
            #[cfg(feature = "benchmark")]
            component_graph,
        }
    }

    fn topological_sort_causal_component(
        &self,
        members: &[Digest],
        causal_outgoing: &HashMap<Digest, Vec<Digest>>,
        node_rank: &HashMap<Digest, usize>,
    ) -> Vec<Digest> {
        let mut indegree: HashMap<Digest, usize> =
            members.iter().cloned().map(|digest| (digest, 0)).collect();
        for member in members {
            for to in &causal_outgoing[member] {
                *indegree
                    .get_mut(to)
                    .expect("causal endpoint must be in component") += 1;
            }
        }

        let mut ready = BinaryHeap::new();
        for (digest, &degree) in &indegree {
            if degree == 0 {
                ready.push(Reverse((node_rank[digest], digest.clone())));
            }
        }
        let mut result = Vec::with_capacity(members.len());

        while let Some(Reverse((_, current))) = ready.pop() {
            result.push(current.clone());

            for next in causal_outgoing
                .get(&current)
                .expect("component vertex must have outgoing entry")
            {
                let degree = indegree
                    .get_mut(next)
                    .expect("component vertex must have indegree entry");
                *degree -= 1;
                if *degree == 0 {
                    ready.push(Reverse((node_rank[next], next.clone())));
                }
            }
        }

        assert_eq!(
            result.len(),
            members.len(),
            "hard causal subgraph must be acyclic"
        );
        result
    }

    fn tie_break(&self, a: &Digest, b: &Digest) -> Ordering {
        let ca = self
            .store
            .get(a)
            .expect(
                "left slice member certificate missing from MRV store during deterministic tie-breaking",
            );
        let cb = self
            .store
            .get(b)
            .expect(
                "right slice member certificate missing from MRV store during deterministic tie-breaking",
            );
        ca.round()
            .cmp(&cb.round())
            .then_with(|| ca.origin().cmp(&cb.origin()))
            .then_with(|| a.cmp(b))
    }

    fn find_scc(
        &self,
        nodes: &[Digest],
        graph: &HashMap<Digest, HashSet<Digest>>,
    ) -> Vec<Vec<Digest>> {
        let mut visited = HashSet::new();
        let mut finish_stack = Vec::new();

        for node in nodes {
            if !visited.contains(node) {
                Self::dfs_finish_iterative(node, graph, &mut visited, &mut finish_stack);
            }
        }

        let mut reverse_graph: HashMap<Digest, Vec<Digest>> =
            nodes.iter().cloned().map(|d| (d, Vec::new())).collect();
        for (from, tos) in graph {
            for to in tos {
                reverse_graph
                    .entry(to.clone())
                    .or_default()
                    .push(from.clone());
            }
        }
        visited.clear();
        let mut components = Vec::new();
        while let Some(node) = finish_stack.pop() {
            if visited.contains(&node) {
                continue;
            }
            components.push(Self::collect_reverse_component(
                node,
                &reverse_graph,
                &mut visited,
            ));
        }
        components
    }

    fn dfs_finish_iterative(
        start: &Digest,
        graph: &HashMap<Digest, HashSet<Digest>>,
        visited: &mut HashSet<Digest>,
        finish_stack: &mut Vec<Digest>,
    ) {
        let mut stack = vec![(start.clone(), false)];
        while let Some((node, exiting)) = stack.pop() {
            if exiting {
                finish_stack.push(node);
                continue;
            }
            if !visited.insert(node.clone()) {
                continue;
            }

            stack.push((node.clone(), true));
            if let Some(neighbors) = graph.get(&node) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push((neighbor.clone(), false));
                    }
                }
            }
        }
    }

    fn collect_reverse_component(
        start: Digest,
        reverse_graph: &HashMap<Digest, Vec<Digest>>,
        visited: &mut HashSet<Digest>,
    ) -> Vec<Digest> {
        let mut component = Vec::new();
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            if !visited.insert(node.clone()) {
                continue;
            }
            component.push(node.clone());
            if let Some(neighbors) = reverse_graph.get(&node) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }
        component
    }

    #[cfg(feature = "benchmark")]
    fn component_reachability(
        component_count: usize,
        graph: &HashMap<usize, HashSet<usize>>,
    ) -> Vec<HashSet<usize>> {
        (0..component_count)
            .map(|start| {
                let mut reachable = HashSet::new();
                let mut stack: Vec<usize> = graph[&start].iter().copied().collect();
                while let Some(current) = stack.pop() {
                    if !reachable.insert(current) {
                        continue;
                    }
                    stack.extend(graph[&current].iter().copied());
                }
                reachable
            })
            .collect()
    }

    #[cfg(feature = "benchmark")]
    fn pair_is_constrained(
        a: &Digest,
        b: &Digest,
        node_to_component: &HashMap<Digest, usize>,
        component_reachability: &[HashSet<usize>],
    ) -> bool {
        let component_a = node_to_component[a];
        let component_b = node_to_component[b];
        component_a != component_b
            && (component_reachability[component_a].contains(&component_b)
                || component_reachability[component_b].contains(&component_a))
    }

    #[cfg(feature = "benchmark")]
    fn count_constrained_pairs(
        incomparable_pairs: &[(Digest, Digest)],
        node_to_component: &HashMap<Digest, usize>,
        component_reachability: &[HashSet<usize>],
    ) -> usize {
        incomparable_pairs
            .iter()
            .filter(|(a, b)| {
                Self::pair_is_constrained(a, b, node_to_component, component_reachability)
            })
            .count()
    }

    #[cfg(feature = "benchmark")]
    fn collect_intervention_metrics(
        &self,
        base_order: &[Digest],
        mrv_order: &[Digest],
        incomparable_pairs: &[(Digest, Digest)],
        node_to_component: &HashMap<Digest, usize>,
        component_reachability: &[HashSet<usize>],
        metrics: &mut OrderingMetrics,
    ) {
        let base_positions: HashMap<&Digest, usize> = base_order
            .iter()
            .enumerate()
            .map(|(position, digest)| (digest, position))
            .collect();
        let mrv_positions: HashMap<&Digest, usize> = mrv_order
            .iter()
            .enumerate()
            .map(|(position, digest)| (digest, position))
            .collect();

        for (a, b) in incomparable_pairs {
            let inverted =
                (base_positions[a] < base_positions[b]) != (mrv_positions[a] < mrv_positions[b]);
            if !inverted {
                continue;
            }
            metrics.incomparable_pair_inversion_count += 1;
            if Self::pair_is_constrained(a, b, node_to_component, component_reachability) {
                metrics.constrained_inversion_count += 1;
            } else {
                metrics.unconstrained_inversion_count += 1;
            }
        }
        metrics.moved_vertex_count = base_order
            .iter()
            .filter(|digest| base_positions[*digest] != mrv_positions[*digest])
            .count();
        metrics.unchanged_slice = base_order == mrv_order;

        let denominator = base_order.len().saturating_sub(1).max(1) as f64;
        let mut displacements: Vec<f64> = base_order
            .iter()
            .map(|digest| {
                base_positions[digest].abs_diff(mrv_positions[digest]) as f64 / denominator
            })
            .collect();
        displacements.sort_by(|a, b| a.partial_cmp(b).expect("displacements cannot be NaN"));

        if !displacements.is_empty() {
            let middle = displacements.len() / 2;
            metrics.position_displacement_median = if displacements.len() % 2 == 0 {
                (displacements[middle - 1] + displacements[middle]) / 2.0
            } else {
                displacements[middle]
            };
            let p95_index = ((displacements.len() as f64 * 0.95).ceil() as usize)
                .saturating_sub(1)
                .min(displacements.len() - 1);
            metrics.position_displacement_p95 = displacements[p95_index];
        }
    }

    #[cfg(feature = "benchmark")]
    fn log_slice_metrics(&self, slice_id: u64, slice: &SliceState, sealed: &SealedMetrics) {
        let metrics = &sealed.ordering;
        info!(
            "MRV_SliceStats slice_id={} mrv_window={} slice_size={} slice_max_round={} seal_horizon={} seal_wait_rounds={} seal_delay_ms={} release_delay_ms={} snapshot_frontier={} trigger_slice_id={} trigger_export_prefix_digest={:?} execution_order_digest={:?} eligible_vertex_count={} ineligible_vertex_count={} all_pair_count={} causal_pair_count={} incomparable_pair_count={} eligible_pair_count={} ineligible_pair_count={} edge_count={} conflict_count={} no_signal_count={} intra_scc_ordering_edge_count={} inter_scc_ordering_edge_count={} constrained_pair_count={} scc_count={} nontrivial_scc_count={} vertices_in_nontrivial_sccs={} max_scc_size={} incomparable_pair_inversion_count={} constrained_inversion_count={} unconstrained_inversion_count={} moved_vertex_count={} unchanged_slice={} position_displacement_median={:.6} position_displacement_p95={:.6}",
            slice_id,
            self.mrv_window,
            metrics.eligible_vertex_count + metrics.ineligible_vertex_count,
            slice.max_round,
            slice.seal_horizon,
            sealed.snapshot_round.saturating_sub(slice.max_round),
            sealed.seal_delay_ms,
            "unavailable",
            sealed.snapshot_round,
            sealed.trigger_slice_id,
            sealed.trigger_export_prefix_digest,
            sealed.execution_order_digest,
            metrics.eligible_vertex_count,
            metrics.ineligible_vertex_count,
            metrics.all_pair_count,
            metrics.causal_pair_count,
            metrics.incomparable_pair_count,
            metrics.eligible_pair_count,
            metrics.ineligible_pair_count,
            metrics.edge_count,
            metrics.conflict_count,
            metrics.no_signal_count,
            metrics.intra_scc_ordering_edge_count,
            metrics.inter_scc_ordering_edge_count,
            metrics.constrained_pair_count,
            metrics.scc_count,
            metrics.nontrivial_scc_count,
            metrics.vertices_in_nontrivial_sccs,
            metrics.max_scc_size,
            metrics.incomparable_pair_inversion_count,
            metrics.constrained_inversion_count,
            metrics.unconstrained_inversion_count,
            metrics.moved_vertex_count,
            metrics.unchanged_slice,
            metrics.position_displacement_median,
            metrics.position_displacement_p95,
        );
    }
}

#[cfg(feature = "benchmark")]
fn fingerprint_digests(domain: &[u8], digests: &[Digest]) -> Digest {
    let mut hasher = Sha512::new();
    hasher.update(domain);
    hasher.update((digests.len() as u64).to_le_bytes());
    for digest in digests {
        hasher.update(digest.as_ref());
    }
    crypto::Digest(
        hasher.finalize()[..32]
            .try_into()
            .expect("SHA-512 output must contain 32 bytes"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use primary::Header;
    #[cfg(feature = "benchmark")]
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
    use tokio::sync::mpsc::{channel, error::TryRecvError};

    fn public_key(value: u8) -> PublicKey {
        PublicKey([value; 32])
    }

    fn raw_digest(value: u8) -> Digest {
        crypto::Digest([value; 32])
    }

    fn certificate(
        id: u8,
        round: Round,
        author: u8,
        parents: impl IntoIterator<Item = Digest>,
    ) -> Certificate {
        Certificate {
            header: Header {
                author: public_key(author),
                round,
                parents: parents.into_iter().collect(),
                id: raw_digest(id),
                ..Header::default()
            },
            ..Certificate::default()
        }
    }

    fn test_executor(
        committee_size: usize,
        mrv_window: Round,
    ) -> (MrvExecutor, Receiver<Certificate>) {
        let (_tx_input, rx_input) = channel(16);
        let (tx_output, rx_output) = channel(16);
        let n = committee_size.max(1);
        let f = n.saturating_sub(1) / 3;
        (
            MrvExecutor {
                rx_input,
                tx_output,
                store: HashMap::new(),
                auf_states: HashMap::new(),
                slices: BTreeMap::new(),
                active_floor_round: None,
                frontier_round: 0,
                mrv_window: mrv_window.max(1),
                reach_threshold: 2 * f + 1,
                delta_threshold: (f + 1) as i64,
                #[cfg(feature = "benchmark")]
                seen_slice_members: HashMap::new(),
                #[cfg(feature = "benchmark")]
                cumulative_member_count: 0,
                #[cfg(feature = "benchmark")]
                export_prefix_digest: Digest::default(),
            },
            rx_output,
        )
    }

    fn insert_target(executor: &mut MrvExecutor, certificate: Certificate) -> Digest {
        let digest = certificate.digest();
        let round = certificate.round();
        executor.store.insert(digest.clone(), certificate);
        executor
            .auf_states
            .insert(digest.clone(), AufState::new(round));
        executor.note_active_round(round);
        digest
    }

    fn set_seen_count(executor: &mut MrvExecutor, digest: &Digest, round: Round, count: usize) {
        let creators = (0..count)
            .map(|index| public_key(index as u8 + 1))
            .collect();
        executor
            .auf_states
            .get_mut(digest)
            .expect("test target must exist")
            .seen_by_round
            .insert(round, creators);
    }

    fn clear_seen(executor: &mut MrvExecutor, digests: &[&Digest]) {
        for digest in digests {
            executor
                .auf_states
                .get_mut(digest)
                .expect("test target must exist")
                .seen_by_round
                .clear();
        }
    }

    fn committed_slice(slice_id: u64, certificates: Vec<Certificate>) -> CommittedSubDag {
        let leader = certificates.last().expect("test slice must be nonempty");
        CommittedSubDag {
            batch_index: slice_id,
            leader_round: leader.round(),
            leader_digest: leader.digest(),
            certificates,
        }
    }

    #[cfg(feature = "benchmark")]
    #[test]
    fn diagnostic_fingerprints_separate_member_set_and_orders() {
        let a = raw_digest(1);
        let b = raw_digest(2);
        let first_order = vec![a.clone(), b.clone()];
        let second_order = vec![b, a];
        let mut first_set = first_order.clone();
        first_set.sort();
        let mut second_set = second_order.clone();
        second_set.sort();

        assert_eq!(
            fingerprint_digests(b"MRV-MEMBER-SET-v1", &first_set),
            fingerprint_digests(b"MRV-MEMBER-SET-v1", &second_set),
        );
        assert_ne!(
            fingerprint_digests(b"MRV-MEMBER-ORDER-v1", &first_order),
            fingerprint_digests(b"MRV-MEMBER-ORDER-v1", &second_order),
        );
        assert_ne!(
            fingerprint_digests(b"MRV-MEMBER-ORDER-v1", &first_order),
            fingerprint_digests(b"MRV-EXECUTION-ORDER-v1", &first_order),
        );
    }

    #[cfg(feature = "benchmark")]
    #[tokio::test]
    #[should_panic(expected = "MRV exact-once membership invariant violated")]
    async fn duplicate_slice_membership_fails_immediately() {
        let (mut executor, _) = test_executor(1, 2);
        let member = certificate(1, 1, 1, []);
        executor
            .on_committed_slice(committed_slice(1, vec![member.clone()]))
            .await
            .unwrap();
        executor
            .on_committed_slice(committed_slice(2, vec![member]))
            .await
            .unwrap();
    }

    #[cfg(feature = "benchmark")]
    #[test]
    #[should_panic(expected = "MRV unsealed member is missing visibility state")]
    fn lifecycle_check_rejects_missing_unsealed_visibility_state() {
        let (mut executor, _) = test_executor(1, 2);
        let member_certificate = certificate(1, 1, 1, []);
        let member = member_certificate.digest();
        executor.store.insert(member.clone(), member_certificate);
        executor.slices.insert(
            1,
            SliceState {
                members: vec![member],
                max_round: 1,
                seal_horizon: 3,
                sealed: None,
                registered_at: Instant::now(),
            },
        );

        executor.assert_member_lifecycle();
    }

    #[cfg(feature = "benchmark")]
    #[test]
    fn controlled_visibility_experiment() {
        const TRIALS: u64 = 100;
        const TARGET_ROUND: Round = 5;
        const FIRST_WINDOW_ROUND: Round = TARGET_ROUND + 1;

        for scenario in ["one_sided", "symmetric", "reversing", "byzantine_only"] {
            let mut expected_edge_count = 0;
            let mut wrong_edge_count = 0;
            let mut no_signal_count = 0;
            let mut conflict_count = 0;
            let mut strict_final_order_count = 0;
            let mut opposite_final_order_count = 0;
            let mut key_opposed_count = 0;
            let mut base_opposed_count = 0;

            for seed in 0..TRIALS {
                let mut rng = StdRng::seed_from_u64(seed);
                let mut creators = [1u8, 2, 3, 4];
                creators.shuffle(&mut rng);
                let (mut executor, _) = test_executor(4, 4);

                let a = insert_target(
                    &mut executor,
                    certificate(rng.gen(), TARGET_ROUND, creators[0], []),
                );
                let b = insert_target(
                    &mut executor,
                    certificate(rng.gen(), TARGET_ROUND, creators[1], []),
                );
                let decoy = insert_target(
                    &mut executor,
                    certificate(rng.gen(), TARGET_ROUND, creators[2], []),
                );

                let a_is_key_first = executor.tie_break(&a, &b) == Ordering::Less;
                let favor_a = if seed % 2 == 0 {
                    a_is_key_first
                } else {
                    !a_is_key_first
                };
                if scenario == "one_sided" && favor_a != a_is_key_first {
                    key_opposed_count += 1;
                }
                let (favored, other) = if favor_a { (&a, &b) } else { (&b, &a) };

                let mut neutral = Vec::new();
                for &creator in &creators {
                    let background = certificate(rng.gen(), TARGET_ROUND - 1, creator, []);
                    let digest = background.digest();
                    executor.store.insert(digest.clone(), background);
                    neutral.push(digest);
                }

                for round in FIRST_WINDOW_ROUND..=TARGET_ROUND + executor.mrv_window {
                    for (creator_index, &creator) in creators.iter().enumerate() {
                        let (see_favored, see_other) = match scenario {
                            // Correct creators favor one endpoint; the faulty
                            // creator favors the other. Later rounds make both
                            // eligible without undoing the first crossing.
                            "one_sided" if round == FIRST_WINDOW_ROUND => {
                                (creator_index < 3, creator_index == 3)
                            }
                            // This is a synthetic committed-DAG stress case for
                            // the full-window Conflict rule, not a claim about
                            // how often reversal occurs in a Narwhal deployment.
                            "reversing" if round == FIRST_WINDOW_ROUND => {
                                (creator_index < 3, creator_index == 3)
                            }
                            "reversing" if round == FIRST_WINDOW_ROUND + 1 => {
                                (creator_index == 3, creator_index < 3)
                            }
                            // The three correct creators see both endpoints;
                            // only the one faulty creator contributes net bias.
                            "byzantine_only" => (true, creator_index < 3),
                            _ => (true, true),
                        };
                        let mut parents = vec![neutral.choose(&mut rng).unwrap().clone()];
                        if see_favored {
                            parents.push(favored.clone());
                        }
                        if see_other {
                            parents.push(other.clone());
                        }
                        let observer = certificate(rng.gen(), round, creator, parents);
                        let digest = observer.digest();
                        let author = observer.origin();
                        executor.frontier_round = executor.frontier_round.max(round);
                        executor.store.insert(digest.clone(), observer);
                        executor.update_seen_for_new_certificate(round, author, &digest);
                    }
                }

                let eligible_a = executor.is_eligible(&a);
                let eligible_b = executor.is_eligible(&b);
                assert!(
                    eligible_a && eligible_b,
                    "controlled scenario must keep both endpoints eligible: scenario={scenario} seed={seed}"
                );
                let verdict = executor.compare_incomparable_pair(&a, &b, eligible_a, eligible_b);
                let expected_verdict = if favor_a {
                    PairVerdict::EdgeAToB
                } else {
                    PairVerdict::EdgeBToA
                };

                match verdict {
                    PairVerdict::EdgeAToB | PairVerdict::EdgeBToA => {
                        if scenario == "one_sided" && verdict == expected_verdict {
                            expected_edge_count += 1;
                        } else {
                            wrong_edge_count += 1;
                        }
                    }
                    PairVerdict::NoSignal => no_signal_count += 1,
                    PairVerdict::Conflict => conflict_count += 1,
                    PairVerdict::Ineligible => unreachable!("eligibility was checked above"),
                }

                let base_opposed = seed % 4 < 2;
                let members = if base_opposed {
                    if scenario == "one_sided" {
                        base_opposed_count += 1;
                    }
                    vec![other.clone(), decoy, favored.clone()]
                } else {
                    vec![favored.clone(), decoy, other.clone()]
                };
                let ordering = executor.order_slice(&members);
                let favored_component = ordering
                    .ordered_sccs
                    .iter()
                    .position(|component| component.contains(favored))
                    .unwrap();
                let other_component = ordering
                    .ordered_sccs
                    .iter()
                    .position(|component| component.contains(other))
                    .unwrap();
                let favored_position = ordering.order.iter().position(|x| x == favored).unwrap();
                let other_position = ordering.order.iter().position(|x| x == other).unwrap();

                if scenario == "one_sided"
                    && favored_component < other_component
                    && favored_position < other_position
                {
                    strict_final_order_count += 1;
                }
                if scenario == "one_sided" && favored_position > other_position {
                    opposite_final_order_count += 1;
                }
            }

            assert_eq!(
                expected_edge_count + wrong_edge_count + no_signal_count + conflict_count,
                TRIALS as usize
            );
            match scenario {
                "one_sided" => {
                    assert_eq!(expected_edge_count, TRIALS as usize);
                    assert_eq!(strict_final_order_count, TRIALS as usize);
                    assert_eq!(opposite_final_order_count, 0);
                    assert_eq!(key_opposed_count, (TRIALS / 2) as usize);
                    assert_eq!(base_opposed_count, (TRIALS / 2) as usize);
                }
                "symmetric" | "byzantine_only" => {
                    assert_eq!(no_signal_count, TRIALS as usize);
                }
                "reversing" => assert_eq!(conflict_count, TRIALS as usize),
                _ => unreachable!(),
            }
            assert_eq!(wrong_edge_count, 0);

            println!(
                "MRV_ControlledVisibilityStats scenario={} trials={} expected_edge_count={} wrong_edge_count={} no_signal_count={} conflict_count={} strict_final_order_count={} opposite_final_order_count={} key_opposed_count={} base_opposed_count={}",
                scenario,
                TRIALS,
                expected_edge_count,
                wrong_edge_count,
                no_signal_count,
                conflict_count,
                strict_final_order_count,
                opposite_final_order_count,
                key_opposed_count,
                base_opposed_count,
            );
        }
    }

    #[tokio::test]
    async fn evidence_is_round_ordered_without_changing_the_base_sequence() {
        let (mut executor, _) = test_executor(1, 2);
        let target_certificate = certificate(1, 1, 1, []);
        let target = target_certificate.digest();
        let descendant_certificate = certificate(2, 2, 2, [target.clone()]);
        let descendant = descendant_certificate.digest();

        executor
            .on_committed_slice(committed_slice(
                1,
                vec![descendant_certificate, target_certificate],
            ))
            .await
            .unwrap();

        assert_eq!(executor.seen_count_at(&target, 2), 1);
        assert_eq!(
            executor.slices.get(&1).unwrap().members,
            vec![descendant, target]
        );
    }

    #[test]
    fn eligibility_uses_the_full_fixed_interval() {
        let (mut executor, _) = test_executor(4, 3);
        let target = insert_target(&mut executor, certificate(1, 5, 1, []));
        set_seen_count(&mut executor, &target, 8, 3);
        set_seen_count(&mut executor, &target, 9, 4);

        assert!(executor.is_eligible(&target));
        executor
            .auf_states
            .get_mut(&target)
            .unwrap()
            .seen_by_round
            .remove(&8);
        assert!(!executor.is_eligible(&target));
    }

    #[test]
    fn comparison_window_starts_after_coexistence_round() {
        let (mut executor, _) = test_executor(4, 3);
        let a = insert_target(&mut executor, certificate(1, 5, 1, []));
        let b = insert_target(&mut executor, certificate(2, 5, 2, []));
        set_seen_count(&mut executor, &a, 5, 3);

        assert_eq!(
            executor.compare_incomparable_pair(&a, &b, true, true),
            PairVerdict::NoSignal
        );
    }

    #[test]
    fn fixed_window_does_not_stop_after_first_crossing() {
        let (mut executor, _) = test_executor(4, 3);
        let a = insert_target(&mut executor, certificate(1, 5, 1, []));
        let b = insert_target(&mut executor, certificate(2, 5, 2, []));
        set_seen_count(&mut executor, &a, 6, 3);
        set_seen_count(&mut executor, &b, 8, 3);

        assert_eq!(
            executor.compare_incomparable_pair(&a, &b, true, true),
            PairVerdict::Conflict
        );
    }

    #[test]
    fn pair_verdicts_cover_ineligible_edges_conflict_and_no_signal() {
        let (mut executor, _) = test_executor(4, 3);
        let a = insert_target(&mut executor, certificate(1, 5, 1, []));
        let b = insert_target(&mut executor, certificate(2, 5, 2, []));

        assert_eq!(
            executor.compare_incomparable_pair(&a, &b, false, true),
            PairVerdict::Ineligible
        );
        assert_eq!(
            executor.compare_incomparable_pair(&a, &b, true, true),
            PairVerdict::NoSignal
        );

        set_seen_count(&mut executor, &a, 6, 3);
        assert_eq!(
            executor.compare_incomparable_pair(&a, &b, true, true),
            PairVerdict::EdgeAToB
        );

        clear_seen(&mut executor, &[&a, &b]);
        set_seen_count(&mut executor, &b, 6, 3);
        assert_eq!(
            executor.compare_incomparable_pair(&a, &b, true, true),
            PairVerdict::EdgeBToA
        );

        set_seen_count(&mut executor, &a, 7, 3);
        assert_eq!(
            executor.compare_incomparable_pair(&a, &b, true, true),
            PairVerdict::Conflict
        );
    }

    #[test]
    fn causal_edge_has_the_hard_direction_even_when_ineligible() {
        let (mut executor, _) = test_executor(4, 2);
        let ancestor_certificate = certificate(1, 2, 1, []);
        let ancestor = ancestor_certificate.digest();
        let descendant_certificate = certificate(2, 3, 2, [ancestor.clone()]);
        let descendant = descendant_certificate.digest();
        insert_target(&mut executor, ancestor_certificate);
        insert_target(&mut executor, descendant_certificate);

        assert!(!executor.is_eligible(&ancestor));
        assert!(!executor.is_eligible(&descendant));
        assert_eq!(
            executor.causal_edge(&descendant, &ancestor),
            Some((ancestor.clone(), descendant.clone()))
        );

        let ordered = executor.order_slice(&[descendant.clone(), ancestor.clone()]);
        assert_eq!(ordered.order, vec![ancestor, descendant]);
        #[cfg(feature = "benchmark")]
        {
            assert_eq!(ordered.metrics.causal_pair_count, 1);
            assert_eq!(ordered.metrics.incomparable_pair_count, 0);
        }
    }

    #[test]
    fn same_round_vertices_skip_impossible_ancestry() {
        let (mut executor, _) = test_executor(4, 2);
        let a_certificate = certificate(1, 2, 1, []);
        let a = a_certificate.digest();
        let b_certificate = certificate(2, 2, 2, [a.clone()]);
        let b = b_certificate.digest();
        insert_target(&mut executor, a_certificate);
        insert_target(&mut executor, b_certificate);

        assert_eq!(executor.causal_edge(&a, &b), None);
    }

    #[test]
    #[should_panic(
        expected = "left slice member certificate missing from MRV store during causal comparison"
    )]
    fn causal_comparison_rejects_a_missing_slice_member() {
        let (executor, _) = test_executor(4, 2);
        executor.causal_edge(&raw_digest(1), &raw_digest(2));
    }

    #[test]
    #[should_panic(
        expected = "left slice member certificate missing from MRV store during deterministic tie-breaking"
    )]
    fn tie_break_rejects_a_missing_slice_member() {
        let (executor, _) = test_executor(4, 2);
        executor.tie_break(&raw_digest(1), &raw_digest(2));
    }

    #[test]
    fn store_gc_keeps_active_ancestry_and_drops_old_history() {
        let (mut executor, _) = test_executor(4, 2);
        let old = certificate(1, 1, 1, []);
        let old_digest = old.digest();
        executor.store.insert(old_digest.clone(), old);

        let target = insert_target(&mut executor, certificate(2, 5, 2, []));
        let intermediate = certificate(3, 6, 3, [target.clone()]);
        let intermediate_digest = intermediate.digest();
        executor
            .store
            .insert(intermediate_digest.clone(), intermediate);

        executor.garbage_collect_store();

        assert!(!executor.store.contains_key(&old_digest));
        assert!(executor.store.contains_key(&target));
        assert!(executor.store.contains_key(&intermediate_digest));
    }

    #[test]
    fn ordering_edge_cycle_is_one_scc() {
        let (mut executor, _) = test_executor(4, 2);
        let a = insert_target(&mut executor, certificate(1, 1, 1, []));
        let b = insert_target(&mut executor, certificate(2, 1, 2, []));
        let c = insert_target(&mut executor, certificate(3, 1, 3, []));
        let nodes = vec![a.clone(), b.clone(), c.clone()];
        let mut graph: HashMap<Digest, HashSet<Digest>> = nodes
            .iter()
            .cloned()
            .map(|node| (node, HashSet::new()))
            .collect();
        graph.get_mut(&a).unwrap().insert(b.clone());
        graph.get_mut(&b).unwrap().insert(c.clone());
        graph.get_mut(&c).unwrap().insert(a.clone());

        let linearization = executor.linearize_graph(&nodes, &graph, &HashSet::new());
        assert_eq!(linearization.ordered_sccs.len(), 1);
        let mut expected = nodes;
        expected.sort_by(|left, right| executor.tie_break(left, right));
        assert_eq!(linearization.order, expected);
    }

    #[test]
    fn scc_internal_order_uses_causal_topological_sort() {
        let (mut executor, _) = test_executor(4, 2);
        let low_key = insert_target(&mut executor, certificate(1, 1, 1, []));
        let causal_first = insert_target(&mut executor, certificate(2, 2, 2, []));
        let third = insert_target(&mut executor, certificate(3, 3, 3, []));
        let nodes = vec![low_key.clone(), causal_first.clone(), third.clone()];
        let mut graph: HashMap<Digest, HashSet<Digest>> = nodes
            .iter()
            .cloned()
            .map(|node| (node, HashSet::new()))
            .collect();
        graph
            .get_mut(&causal_first)
            .unwrap()
            .insert(low_key.clone());
        graph.get_mut(&low_key).unwrap().insert(third.clone());
        graph.get_mut(&third).unwrap().insert(causal_first.clone());
        let causal_edges = HashSet::from([(causal_first.clone(), low_key.clone())]);

        let linearization = executor.linearize_graph(&nodes, &graph, &causal_edges);
        let first_position = linearization
            .order
            .iter()
            .position(|digest| digest == &causal_first)
            .unwrap();
        let low_position = linearization
            .order
            .iter()
            .position(|digest| digest == &low_key)
            .unwrap();
        assert!(first_position < low_position);
    }

    #[test]
    fn zero_indegree_sccs_and_vertices_use_fixed_keys() {
        let (mut executor, _) = test_executor(4, 2);
        let late = insert_target(&mut executor, certificate(3, 3, 3, []));
        let early_b = insert_target(&mut executor, certificate(2, 1, 2, []));
        let early_a = insert_target(&mut executor, certificate(1, 1, 1, []));
        let nodes = vec![late.clone(), early_b.clone(), early_a.clone()];
        let graph: HashMap<Digest, HashSet<Digest>> = nodes
            .iter()
            .cloned()
            .map(|node| (node, HashSet::new()))
            .collect();

        let linearization = executor.linearize_graph(&nodes, &graph, &HashSet::new());
        let mut expected = nodes;
        expected.sort_by(|left, right| executor.tie_break(left, right));
        assert_eq!(linearization.order, expected);
        assert_eq!(linearization.ordered_sccs.len(), 3);
    }

    #[test]
    fn nontrivial_scc_uses_its_minimum_member_key() {
        let (mut executor, _) = test_executor(4, 2);
        let low = insert_target(&mut executor, certificate(1, 1, 1, []));
        let high = insert_target(&mut executor, certificate(3, 3, 3, []));
        let middle = insert_target(&mut executor, certificate(2, 2, 2, []));
        let nodes = vec![middle.clone(), high.clone(), low.clone()];
        let graph = HashMap::from([
            (low.clone(), HashSet::from([high.clone()])),
            (high.clone(), HashSet::from([low.clone()])),
            (middle.clone(), HashSet::new()),
        ]);

        let linearization = executor.linearize_graph(&nodes, &graph, &HashSet::new());

        assert_eq!(linearization.ordered_sccs[0], vec![low, high]);
        assert_eq!(linearization.ordered_sccs[1], vec![middle]);
    }

    #[cfg(feature = "benchmark")]
    #[test]
    fn constrained_pairs_exclude_same_scc_and_include_direct_and_transitive_paths() {
        let a = raw_digest(1);
        let b = raw_digest(2);
        let c = raw_digest(3);
        let d = raw_digest(4);
        let e = raw_digest(5);
        let node_to_component = HashMap::from([
            (a.clone(), 0),
            (b.clone(), 0),
            (c.clone(), 1),
            (d.clone(), 2),
            (e.clone(), 3),
        ]);
        let component_graph = HashMap::from([
            (0, HashSet::from([1])),
            (1, HashSet::from([2])),
            (2, HashSet::new()),
            (3, HashSet::new()),
        ]);
        let incomparable_pairs = vec![(a.clone(), b), (a, d.clone()), (c.clone(), d), (c, e)];
        let reachability = MrvExecutor::component_reachability(4, &component_graph);

        assert_eq!(
            MrvExecutor::count_constrained_pairs(
                &incomparable_pairs,
                &node_to_component,
                &reachability,
            ),
            2
        );
    }

    #[cfg(feature = "benchmark")]
    #[test]
    fn intervention_metrics_compare_the_exporter_sequence() {
        let (executor, _) = test_executor(4, 2);
        let a = raw_digest(1);
        let b = raw_digest(2);
        let c = raw_digest(3);
        let d = raw_digest(4);
        let base = vec![a.clone(), b.clone(), c.clone(), d.clone()];
        let mrv = vec![b.clone(), a.clone(), d.clone(), c.clone()];
        let incomparable_pairs = vec![(a.clone(), b.clone()), (c.clone(), d.clone())];
        let node_to_component = HashMap::from([(a, 0), (b, 1), (c, 2), (d, 3)]);
        let component_graph = HashMap::from([
            (0, HashSet::new()),
            (1, HashSet::from([0])),
            (2, HashSet::new()),
            (3, HashSet::new()),
        ]);
        let reachability = MrvExecutor::component_reachability(4, &component_graph);
        let mut metrics = OrderingMetrics::default();

        executor.collect_intervention_metrics(
            &base,
            &mrv,
            &incomparable_pairs,
            &node_to_component,
            &reachability,
            &mut metrics,
        );

        assert_eq!(metrics.incomparable_pair_inversion_count, 2);
        assert_eq!(metrics.constrained_inversion_count, 1);
        assert_eq!(metrics.unconstrained_inversion_count, 1);
        assert_eq!(metrics.moved_vertex_count, 4);
        assert!(!metrics.unchanged_slice);
        assert_eq!(metrics.position_displacement_median, 1.0 / 3.0);
        assert_eq!(metrics.position_displacement_p95, 1.0 / 3.0);
    }

    #[tokio::test]
    async fn sealed_slice_is_not_changed_by_later_prefixes() {
        let (mut executor, mut rx_output) = test_executor(1, 1);
        let target_certificate = certificate(1, 1, 1, []);
        let target = target_certificate.digest();
        executor
            .on_committed_slice(committed_slice(1, vec![target_certificate]))
            .await
            .unwrap();
        assert!(matches!(rx_output.try_recv(), Err(TryRecvError::Empty)));

        executor
            .on_committed_slice(committed_slice(2, vec![certificate(2, 2, 2, [])]))
            .await
            .unwrap();
        assert_eq!(rx_output.recv().await.unwrap().digest(), target);
        assert!(!executor.auf_states.contains_key(&target));
        assert!(!executor.slices.contains_key(&1));

        executor
            .on_committed_slice(committed_slice(3, vec![certificate(3, 3, 3, [])]))
            .await
            .unwrap();
        assert_ne!(rx_output.recv().await.unwrap().digest(), target);
    }

    #[tokio::test]
    async fn sealed_but_blocked_slice_is_not_changed_by_later_evidence() {
        let (mut executor, mut rx_output) = test_executor(1, 2);
        executor
            .on_committed_slice(committed_slice(1, vec![certificate(10, 10, 10, [])]))
            .await
            .unwrap();

        let a_certificate = certificate(1, 1, 1, []);
        let a = a_certificate.digest();
        let b_certificate = certificate(2, 1, 2, []);
        let b = b_certificate.digest();
        executor
            .on_committed_slice(committed_slice(2, vec![a_certificate, b_certificate]))
            .await
            .unwrap();

        let sealed = executor.slices.get(&2).unwrap().sealed.as_ref().unwrap();
        let fixed_order = sealed.order.clone();
        let fixed_sccs = sealed.ordered_sccs.clone();
        assert_eq!(fixed_order, vec![a, b.clone()]);
        assert!(executor.store.contains_key(&b));
        assert!(matches!(rx_output.try_recv(), Err(TryRecvError::Empty)));

        // If slice 2 were recomputed, this later round-2 observer would create
        // B -> A evidence inside its comparison window. A sealed result must
        // remain fixed while slice 1 still blocks exporter-order release.
        executor
            .on_committed_slice(committed_slice(3, vec![certificate(3, 2, 3, [b])]))
            .await
            .unwrap();

        let sealed = executor.slices.get(&2).unwrap().sealed.as_ref().unwrap();
        assert_eq!(sealed.order, fixed_order);
        assert_eq!(sealed.ordered_sccs, fixed_sccs);
        assert!(matches!(rx_output.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn ancestry_walk_crosses_a_sealed_intermediate() {
        let (mut executor, mut rx_output) = test_executor(1, 2);
        let target_certificate = certificate(1, 1, 1, []);
        let target = target_certificate.digest();
        executor
            .on_committed_slice(committed_slice(
                1,
                vec![target_certificate, certificate(10, 10, 10, [])],
            ))
            .await
            .unwrap();

        let intermediate_certificate = certificate(2, 2, 2, [target.clone()]);
        let intermediate = intermediate_certificate.digest();
        executor
            .on_committed_slice(committed_slice(2, vec![intermediate_certificate]))
            .await
            .unwrap();
        assert!(!executor.auf_states.contains_key(&intermediate));
        assert!(executor.store.contains_key(&intermediate));

        executor
            .on_committed_slice(committed_slice(
                3,
                vec![certificate(3, 3, 3, [intermediate])],
            ))
            .await
            .unwrap();

        assert_eq!(executor.seen_count_at(&target, 3), 1);
        assert!(matches!(rx_output.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn release_preserves_exporter_slice_order() {
        let (mut executor, mut rx_output) = test_executor(1, 2);
        let first_certificate = certificate(1, 10, 1, []);
        let first = first_certificate.digest();
        let second_certificate = certificate(2, 1, 2, []);
        let second = second_certificate.digest();

        executor
            .on_committed_slice(committed_slice(1, vec![first_certificate]))
            .await
            .unwrap();
        executor
            .on_committed_slice(committed_slice(2, vec![second_certificate]))
            .await
            .unwrap();
        assert!(executor.slices.get(&2).unwrap().sealed.is_some());
        assert!(matches!(rx_output.try_recv(), Err(TryRecvError::Empty)));

        executor
            .on_committed_slice(committed_slice(3, vec![certificate(3, 12, 3, [])]))
            .await
            .unwrap();
        assert_eq!(rx_output.recv().await.unwrap().digest(), first);
        assert_eq!(rx_output.recv().await.unwrap().digest(), second);
        assert!(!executor.store.contains_key(&first));
        assert!(!executor.store.contains_key(&second));
    }
}
