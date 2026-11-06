// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

#[derive(Clone, Debug)]
pub struct CommittedSubDag {
    pub batch_index: u64,
    pub leader_round: Round,
    pub leader_digest: Digest,
    pub certificates: Vec<Certificate>,
}

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Round,
    // Creator progress is used for the committed frontier, not digest delivery.
    last_committed: HashMap<PublicKey, Round>,
    /// Exact delivery records in the retained round range.
    delivered: HashMap<Digest, Round>,
    /// Certificates retained for leader queries and execution ancestry.
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            delivered: genesis
                .values()
                .map(|(digest, certificate)| (digest.clone(), certificate.round()))
                .collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.delivered.insert(certificate.digest(), certificate.round());
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        self.dag.retain(|r, _| r + gc_depth >= last_committed_round);
        self.delivered
            .retain(|_, r| *r + gc_depth >= last_committed_round);
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,
    // Primary already persists authenticated certificates here. Delivery
    // records below the in-memory GC range use the same archive.
    store: Store,
    delivery_epoch: u64,
    #[cfg(feature = "benchmark")]
    archive_delivery_reads: usize,
    #[cfg(feature = "benchmark")]
    archive_metadata_reads: usize,
    #[cfg(feature = "benchmark")]
    archive_read_us: u128,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<CommittedSubDag>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        store: Store,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<CommittedSubDag>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                store,
                delivery_epoch: 0,
                #[cfg(feature = "benchmark")]
                archive_delivery_reads: 0,
                #[cfg(feature = "benchmark")]
                archive_metadata_reads: 0,
                #[cfg(feature = "benchmark")]
                archive_read_us: 0,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // State starts at genesis on every spawn. Old archive delivery records
        // must not be mistaken for delivery in this fresh execution stream.
        let previous_epoch = self
            .store
            .read(b"exporter-epoch".to_vec())
            .await
            .expect("Failed to read exporter epoch")
            .map(|bytes| bincode::deserialize::<u64>(&bytes).expect("Invalid exporter epoch"))
            .unwrap_or(0);
        self.delivery_epoch = previous_epoch
            .checked_add(1)
            .expect("Exporter epoch exhausted");
        self.store
            .write(
                b"exporter-epoch".to_vec(),
                bincode::serialize(&self.delivery_epoch).unwrap(),
            )
            .await;
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());
        let mut next_batch_index = 0;

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            if round + self.gc_depth < state.last_committed_round
                || state.delivered.contains_key(&certificate.digest())
            {
                debug!("Ignoring already committed certificate {:?}", certificate);
                continue;
            }

            // Add the new certificate to the local storage.
            state
                .dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin(), (certificate.digest(), certificate));

            // Try to order the dag to commit. Start from the highest round for which we have at least
            // 2f+1 certificates. This is because we need them to reveal the common coin.
            let r = round - 1;

            // We only elect leaders for even round numbers.
            if r % 2 != 0 || r < 4 {
                continue;
            }

            // Get the certificate's digest of the leader of round r-2. If we already ordered this leader,
            // there is nothing to do.
            let leader_round = r - 2;
            if leader_round <= state.last_committed_round {
                continue;
            }
            let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check if the leader has f+1 support from its children (ie. round r-1).
            let stake: Stake = state
                .dag
                .get(&(r - 1))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(_, x)| x.header.parents.contains(&leader_digest))
                .map(|(_, x)| self.committee.stake(&x.origin()))
                .sum();

            // If it is the case, we can commit the leader. But first, we need to recursively go back to
            // the last committed leader, and commit all preceding leaders in the right order. Committing
            // a leader block means committing all its dependencies.
            if stake < self.committee.validity_threshold() {
                debug!("Leader {:?} does not have enough support", leader);
                continue;
            }

            // Get an ordered list of past leaders that are linked to the current leader.
            debug!("Leader {:?} has enough support", leader);
            for leader in self.order_leaders(leader, &state).iter().rev() {
                let mut batch = Vec::new();
                #[cfg(feature = "benchmark")]
                let export_started = std::time::Instant::now();
                #[cfg(feature = "benchmark")]
                let mut delivery_enqueue_us = 0;
                #[cfg(feature = "benchmark")]
                {
                    self.archive_delivery_reads = 0;
                    self.archive_metadata_reads = 0;
                    self.archive_read_us = 0;
                }

                // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                for x in self.order_dag(leader, &state).await {
                    #[cfg(feature = "benchmark")]
                    let enqueue_started = std::time::Instant::now();
                    self.store
                        .write(
                            [b"exporter-delivered/".as_ref(), x.digest().as_ref()].concat(),
                            bincode::serialize(&self.delivery_epoch).unwrap(),
                        )
                        .await;
                    #[cfg(feature = "benchmark")]
                    { delivery_enqueue_us += enqueue_started.elapsed().as_micros(); }
                    // Update and clean up internal state.
                    state.update(&x, self.gc_depth);

                    // Add the certificate to the current committed batch.
                    batch.push(x);
                }

                if batch.is_empty() {
                    continue;
                }

                next_batch_index += 1;

                // Log the latest committed round of every authority (for debug).
                if log_enabled!(log::Level::Debug) {
                    for (name, round) in &state.last_committed {
                        debug!("Latest commit of {}: Round {}", name, round);
                    }
                }

                // Output the sequence to the primary in the original certificate order.
                for certificate in &batch {
                    #[cfg(not(feature = "benchmark"))]
                    info!("Committed {}", certificate.header);

                    #[cfg(feature = "benchmark")]
                    for digest in certificate.header.payload.keys() {
                        // NOTE: This log entry is used to compute consensus commit performance.
                        info!("Tusk_Committed {} -> {:?}", certificate.header, digest);
                    }

                    self.tx_primary
                        .send(certificate.clone())
                        .await
                        .expect("Failed to send certificate to primary");
                }

                #[cfg(feature = "benchmark")]
                info!("MRV_ExporterIO batch_index={} leader_round={} certificates={} delivery_writes={} delivery_enqueue_us={} archive_delivery_reads={} archive_metadata_reads={} archive_read_us={} export_us={} cached_delivered={} cached_dag_vertices={}",
                    next_batch_index, leader.round(), batch.len(), batch.len(), delivery_enqueue_us,
                    self.archive_delivery_reads, self.archive_metadata_reads, self.archive_read_us,
                    export_started.elapsed().as_micros(), state.delivered.len(),
                    state.dag.values().map(HashMap::len).sum::<usize>());

                let committed_batch = CommittedSubDag {
                    batch_index: next_batch_index,
                    leader_round: leader.round(),
                    leader_digest: leader.digest(),
                    certificates: batch,
                };

                if let Err(e) = self.tx_output.send(committed_batch).await {
                    warn!("Failed to output committed batch: {}", e);
                }
            }
        }
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        // Elect the leader.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Order the past leaders that we didn't already commit.
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for r in (state.last_committed_round + 2..=leader.round() - 2)
            .rev()
            .step_by(2)
        {
            // Get the certificate proposed by the previous leader.
            let (_, prev_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        to_commit
    }

    /// Checks if there is a path between two leaders.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    async fn order_dag(&mut self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader.clone()];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                if x.round() == 1
                    || already_ordered.contains(parent)
                    || state.delivered.contains_key(parent)
                {
                    continue;
                }
                if x.round() - 1 + self.gc_depth < state.last_committed_round {
                    #[cfg(feature = "benchmark")]
                    let read_started = std::time::Instant::now();
                    let delivered = self.store
                        .read([b"exporter-delivered/".as_ref(), parent.as_ref()].concat())
                        .await
                        .expect("Failed to read archived delivery record");
                    #[cfg(feature = "benchmark")]
                    {
                        self.archive_delivery_reads += 1;
                        self.archive_read_us += read_started.elapsed().as_micros();
                    }
                    if delivered.map_or(false, |bytes| {
                        bincode::deserialize::<u64>(&bytes).expect("Invalid delivery record") == self.delivery_epoch
                    }) {
                        continue;
                    }
                }
                let certificate = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some((_, certificate)) => certificate.clone(),
                    None => {
                        #[cfg(feature = "benchmark")]
                        let read_started = std::time::Instant::now();
                        let bytes = self.store
                            .read(parent.to_vec())
                            .await
                            .expect("Failed to read archived ancestor")
                            .expect("Primary must persist an authenticated ancestor before consensus delivery");
                        #[cfg(feature = "benchmark")]
                        {
                            self.archive_metadata_reads += 1;
                            self.archive_read_us += read_started.elapsed().as_micros();
                        }
                        bincode::deserialize(&bytes).expect("Invalid archived ancestor")
                    }
                };
                buffer.push(certificate);
                already_ordered.insert(parent.clone());
            }
        }

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}
