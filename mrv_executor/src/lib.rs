use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use consensus::CommittedSubDag;
use crypto::Hash as _;
use crypto::PublicKey;
use log::{debug, info, warn};
use primary::Certificate;
use tokio::sync::mpsc::{Receiver, Sender};

pub type Round = u64;
pub type Digest = crypto::Digest;

#[derive(Clone)]
struct AufState {
    round: Round,
    seen_by_round: BTreeMap<Round, HashSet<PublicKey>>,
    horizon: Option<Round>,
    mature: bool,
}

impl AufState {
    fn new(round: Round) -> Self {
        Self {
            round,
            seen_by_round: BTreeMap::new(),
            horizon: None,
            mature: false,
        }
    }
}

#[derive(Default)]
struct BatchState {
    members: Vec<Digest>,
}

pub struct MrvExecutor {
    rx_input: Receiver<CommittedSubDag>,
    tx_output: Sender<Certificate>,

    // Full committed DAG metadata that MRV can query.
    store: HashMap<Digest, Certificate>,

    // Active AUFs and active batches that are not finalized yet.
    auf_states: HashMap<Digest, AufState>,
    batches: BTreeMap<u64, BatchState>,
    active_floor_round: Option<Round>,

    frontier_round: Round,

    // MRV system parameters.
    window_cap: Round,
    reach_threshold: usize, // 2f + 1
    delta_threshold: i64,   // f + 1
}

impl MrvExecutor {
    pub fn spawn(
        rx_input: Receiver<CommittedSubDag>,
        tx_output: Sender<Certificate>,
        committee_size: usize,
        window_cap: Round,
    ) {
        let n = committee_size.max(1);
        let f = n.saturating_sub(1) / 3;
        let reach_threshold = 2 * f + 1;
        let delta_threshold = (f + 1) as i64;
        let window_cap = window_cap.max(1);

        tokio::spawn(async move {
            info!(
                "MRV Executor (Stopping-Time) started: n={}, f={}, W_max={}, reach={}, delta={}",
                n, f, window_cap, reach_threshold, delta_threshold
            );

            let mut executor = Self {
                rx_input,
                tx_output,
                store: HashMap::new(),
                auf_states: HashMap::new(),
                batches: BTreeMap::new(),
                active_floor_round: None,
                frontier_round: 0,
                window_cap,
                reach_threshold,
                delta_threshold,
            };
            executor.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some(committed_batch) = self.rx_input.recv().await {
            if self.on_committed_batch(committed_batch).await.is_err() {
                warn!("MRV stopped because downstream receiver was dropped");
                return;
            }
        }
    }

    async fn on_committed_batch(&mut self, committed_batch: CommittedSubDag) -> Result<(), ()> {
        debug!(
            "Processing committed batch index={} leader_round={} leader_digest={:?} size={}",
            committed_batch.batch_index,
            committed_batch.leader_round,
            committed_batch.leader_digest,
            committed_batch.certificates.len()
        );

        let mut members = Vec::new();
        for certificate in committed_batch.certificates {
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
            members.push(digest);
        }

        if !members.is_empty() {
            self.batches
                .insert(committed_batch.batch_index, BatchState { members });
        }

        self.apply_window_cap();
        self.try_finalize_batches().await
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
                let seen = state.seen_by_round.entry(round).or_default();
                if seen.insert(author)
                    && state.horizon.is_none()
                    && seen.len() >= self.reach_threshold
                {
                    state.horizon = Some(round);
                    state.mature = true;
                }
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

    fn apply_window_cap(&mut self) {
        for state in self.auf_states.values_mut() {
            if state.horizon.is_some() {
                continue;
            }

            let cap_round = state.round.saturating_add(self.window_cap);
            if self.frontier_round >= cap_round {
                state.horizon = Some(cap_round);
                state.mature = false;
            }
        }
    }

    async fn try_finalize_batches(&mut self) -> Result<(), ()> {
        loop {
            let next_round = match self.batches.keys().next().copied() {
                Some(r) => r,
                None => return Ok(()),
            };

            let members = match self.batches.get(&next_round) {
                Some(batch) if !batch.members.is_empty() => batch.members.clone(),
                Some(_) => {
                    self.batches.remove(&next_round);
                    continue;
                }
                None => continue,
            };

            let batch_horizon = match self.batch_horizon(&members) {
                Some(h) => h,
                None => return Ok(()),
            };

            if self.frontier_round < batch_horizon {
                return Ok(());
            }

            let sorted = self.sort_batch(&members);
            for digest in sorted {
                if let Some(certificate) = self.store.get(&digest).cloned() {
                    self.tx_output.send(certificate).await.map_err(|_| ())?;
                }
            }

            self.release_batch(next_round, &members);
        }
    }

    fn batch_horizon(&self, members: &[Digest]) -> Option<Round> {
        let mut horizon = 0;
        for digest in members {
            let h = self.auf_states.get(digest)?.horizon?;
            horizon = horizon.max(h);
        }
        Some(horizon)
    }

    fn release_batch(&mut self, batch_index: u64, members: &[Digest]) {
        self.batches.remove(&batch_index);
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

    // ---------------------------------------------------------------------
    // MRV ordering in one batch: frozen pair verdicts -> SCC -> topo -> tie-break
    // ---------------------------------------------------------------------

    fn sort_batch(&self, members: &[Digest]) -> Vec<Digest> {
        if members.len() <= 1 {
            return members.to_vec();
        }

        let mut nodes = members.to_vec();
        nodes.sort_by(|a, b| self.tie_break(a, b));

        let mut graph: HashMap<Digest, HashSet<Digest>> =
            nodes.iter().cloned().map(|d| (d, HashSet::new())).collect();

        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let a = &nodes[i];
                let b = &nodes[j];
                match self.compare_pair(a, b) {
                    OrderingRelation::ABetter => {
                        graph.get_mut(a).expect("node must exist").insert(b.clone());
                    }
                    OrderingRelation::BBetter => {
                        graph.get_mut(b).expect("node must exist").insert(a.clone());
                    }
                    OrderingRelation::Tie => {}
                }
            }
        }

        let components = self.find_scc(&nodes, &graph);

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

        for (from, tos) in &graph {
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

        let mut ready: Vec<usize> = component_indegree
            .iter()
            .filter_map(|(&component_idx, &degree)| (degree == 0).then_some(component_idx))
            .collect();

        let mut result = Vec::new();
        while !ready.is_empty() {
            ready.sort_by(|a, b| self.compare_components(&components[*a], &components[*b]));
            let current = ready.remove(0);

            let mut component_members = components[current].clone();
            component_members.sort_by(|a, b| self.tie_break(a, b));
            result.extend(component_members);

            let mut next_components: Vec<usize> = component_graph
                .get(&current)
                .into_iter()
                .flat_map(|neighbors| neighbors.iter().copied())
                .collect();
            next_components.sort_unstable();

            for next in next_components {
                let degree = component_indegree
                    .get_mut(&next)
                    .expect("component must exist");
                *degree -= 1;
                if *degree == 0 {
                    ready.push(next);
                }
            }
        }

        result
    }

    fn compare_pair(&self, a: &Digest, b: &Digest) -> OrderingRelation {
        let state_a = match self.auf_states.get(a) {
            Some(x) => x,
            None => return OrderingRelation::Tie,
        };
        let state_b = match self.auf_states.get(b) {
            Some(x) => x,
            None => return OrderingRelation::Tie,
        };

        // Capped-but-not-mature AUFs never produce a positive fairness edge.
        if !state_a.mature || !state_b.mature {
            return OrderingRelation::Tie;
        }

        let horizon_a = state_a.horizon.expect("horizon must be finalized");
        let horizon_b = state_b.horizon.expect("horizon must be finalized");
        let pair_horizon = horizon_a.max(horizon_b);

        let coexistence_start = state_a.round.max(state_b.round);
        if pair_horizon <= coexistence_start {
            return OrderingRelation::Tie;
        }

        let mut pos = 0;
        let mut neg = 0;

        for round in (coexistence_start + 1)..=pair_horizon {
            let seen_a = self.seen_count_at(a, round) as i64;
            let seen_b = self.seen_count_at(b, round) as i64;
            let diff = seen_a - seen_b;

            if diff >= self.delta_threshold {
                pos += 1;
            } else if diff <= -self.delta_threshold {
                neg += 1;
            }
        }

        if pos >= 1 && neg == 0 {
            OrderingRelation::ABetter
        } else if neg >= 1 && pos == 0 {
            OrderingRelation::BBetter
        } else {
            OrderingRelation::Tie
        }
    }

    fn seen_count_at(&self, digest: &Digest, round: Round) -> usize {
        self.auf_states
            .get(digest)
            .and_then(|state| state.seen_by_round.get(&round))
            .map_or(0, HashSet::len)
    }

    fn tie_break(&self, a: &Digest, b: &Digest) -> Ordering {
        match (self.store.get(a), self.store.get(b)) {
            (Some(ca), Some(cb)) => ca
                .round()
                .cmp(&cb.round())
                .then_with(|| ca.origin().cmp(&cb.origin()))
                .then_with(|| a.cmp(b)),
            _ => a.cmp(b),
        }
    }

    fn compare_components(&self, c1: &[Digest], c2: &[Digest]) -> Ordering {
        let min1 = c1
            .iter()
            .min_by(|a, b| self.tie_break(a, b))
            .expect("component should not be empty");
        let min2 = c2
            .iter()
            .min_by(|a, b| self.tie_break(a, b))
            .expect("component should not be empty");
        self.tie_break(min1, min2)
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
                self.dfs_forward(node, graph, &mut visited, &mut finish_stack);
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

        for tos in reverse_graph.values_mut() {
            tos.sort_by(|a, b| self.tie_break(a, b));
        }

        visited.clear();
        let mut components = Vec::new();

        while let Some(node) = finish_stack.pop() {
            if visited.contains(&node) {
                continue;
            }

            let mut component = Vec::new();
            self.dfs_reverse(&node, &reverse_graph, &mut visited, &mut component);
            components.push(component);
        }

        components
    }

    fn dfs_forward(
        &self,
        node: &Digest,
        graph: &HashMap<Digest, HashSet<Digest>>,
        visited: &mut HashSet<Digest>,
        finish_stack: &mut Vec<Digest>,
    ) {
        visited.insert(node.clone());

        if let Some(neighbors) = graph.get(node) {
            let mut ordered_neighbors: Vec<&Digest> = neighbors.iter().collect();
            ordered_neighbors.sort_by(|a, b| self.tie_break(a, b));
            for neighbor in ordered_neighbors {
                if !visited.contains(neighbor) {
                    self.dfs_forward(neighbor, graph, visited, finish_stack);
                }
            }
        }

        finish_stack.push(node.clone());
    }

    fn dfs_reverse(
        &self,
        node: &Digest,
        reverse_graph: &HashMap<Digest, Vec<Digest>>,
        visited: &mut HashSet<Digest>,
        component: &mut Vec<Digest>,
    ) {
        visited.insert(node.clone());
        component.push(node.clone());

        if let Some(neighbors) = reverse_graph.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    self.dfs_reverse(neighbor, reverse_graph, visited, component);
                }
            }
        }
    }
}

enum OrderingRelation {
    ABetter,
    BBetter,
    Tie,
}
