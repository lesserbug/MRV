// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--workload=[MODE] 'steady or waves workload pattern'")
        .args_from_usage("--wave-burst-ms=[INT] 'The burst duration in ms for waves workload'")
        .args_from_usage("--wave-gap-ms=[INT] 'The silent gap duration in ms for waves workload'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let workload = Workload::from_matches(&matches)?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let client = Client {
        target,
        size,
        rate,
        workload,
        nodes,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    workload: Workload,
    nodes: Vec<SocketAddr>,
}

#[derive(Clone, Copy)]
enum Workload {
    Steady,
    Waves { burst_ms: u64, gap_ms: u64 },
}

impl Workload {
    const SAMPLE_COUNTER_MASK: u64 = (1u64 << 48) - 1;

    fn from_matches(matches: &ArgMatches<'_>) -> Result<Self> {
        match matches.value_of("workload").unwrap_or("steady") {
            "steady" => Ok(Self::Steady),
            "waves" => {
                let burst_ms = matches
                    .value_of("wave-burst-ms")
                    .unwrap_or("300")
                    .parse::<u64>()
                    .context("The wave burst duration must be a non-negative integer")?;
                let gap_ms = matches
                    .value_of("wave-gap-ms")
                    .unwrap_or("1200")
                    .parse::<u64>()
                    .context("The wave gap duration must be a non-negative integer")?;

                if burst_ms == 0 || gap_ms == 0 {
                    return Err(anyhow::Error::msg(
                        "Waves workload requires both burst and gap durations to be positive",
                    ));
                }

                Ok(Self::Waves { burst_ms, gap_ms })
            }
            other => Err(anyhow::Error::msg(format!(
                "Unsupported workload mode '{}': expected 'steady' or 'waves'",
                other
            ))),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::Waves { .. } => "waves",
        }
    }

    fn is_active(&self, benchmark_start: Instant) -> bool {
        match self {
            Self::Steady => true,
            Self::Waves { burst_ms, gap_ms } => {
                let cycle_ms = *burst_ms + *gap_ms;
                (benchmark_start.elapsed().as_millis() as u64) % cycle_ms < *burst_ms
            }
        }
    }

    fn make_sample_id(&self, benchmark_start: Instant, sample_counter: u64) -> u64 {
        let wave_id = match self {
            Self::Steady => 0,
            Self::Waves { burst_ms, gap_ms } => {
                let cycle_ms = *burst_ms + *gap_ms;
                ((benchmark_start.elapsed().as_millis() as u64) / cycle_ms) as u64
            }
        };

        (wave_id << 48) | (sample_counter & Self::SAMPLE_COUNTER_MASK)
    }
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        if burst == 0 {
            return Err(anyhow::Error::msg(
                "Transaction rate must be at least 20 tx/s",
            ));
        }
        let mut tx = BytesMut::with_capacity(self.size);
        let mut sample_counter = 0;
        let mut r = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        info!("Workload: {}", self.workload.name());
        if let Workload::Waves { burst_ms, gap_ms } = self.workload {
            info!("Wave burst: {} ms", burst_ms);
            info!("Wave gap: {} ms", gap_ms);
        }

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");
        let benchmark_start = Instant::now();

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            if !self.workload.is_active(benchmark_start) {
                continue;
            }

            for x in 0..burst {
                if x == sample_counter % burst {
                    let tx_id = self
                        .workload
                        .make_sample_id(benchmark_start, sample_counter);

                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", tx_id);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(tx_id); // This counter identifies the tx.
                } else {
                    r += 1;
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                };

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            sample_counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
