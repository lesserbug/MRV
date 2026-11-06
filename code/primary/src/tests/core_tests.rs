// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{
    certificate, committee, committee_with_base_port, header, headers, keys, listener, votes,
};
use futures::future::try_join_all;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn waiter_loopback_after_vote_gc_does_not_vote_again() {
    use futures::{SinkExt as _, StreamExt as _};
    let mut fixture_keys = keys();
    let (author, author_secret) = fixture_keys.pop().unwrap();
    let (name, secret) = fixture_keys.pop().unwrap();
    let committee = committee_with_base_port(31_000);
    let first_header = header();
    let mut waiting_header = Header {
        payload: [(Digest([7; 32]), 0)].iter().cloned().collect(),
        ..first_header.clone()
    };
    waiting_header.id = waiting_header.digest();
    waiting_header.signature = crypto::Signature::new(&waiting_header.id, &author_secret);
    first_header.verify(&committee).unwrap();
    waiting_header.verify(&committee).unwrap();
    assert_ne!(first_header.id, waiting_header.id);
    assert_eq!((first_header.author, first_header.round), (author, waiting_header.round));

    let (tx_sync_headers, mut rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary, rx_primary) = channel(1);
    let (tx_loopback, rx_loopback) = channel(1);
    let (_tx_certificates, rx_certificates) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);
    let path = std::env::temp_dir().join(format!("mrv-canonical-waiter-gc-{}-{}",
        std::process::id(), std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    println!("canonical_waiter_gc_store={}", path.display());
    let mut store = Store::new(path.to_str().unwrap()).unwrap();
    let consensus_round = Arc::new(AtomicU64::new(0));
    let mut core = Core {
        name,
        committee: committee.clone(),
        store: store.clone(),
        synchronizer: Synchronizer::new(name, &committee, store.clone(),
            tx_sync_headers, tx_sync_certificates),
        signature_service: SignatureService::new(secret),
        consensus_round: consensus_round.clone(),
        gc_depth: 50,
        rx_primaries: rx_primary,
        rx_header_waiter: rx_loopback,
        rx_certificate_waiter: rx_certificates,
        rx_proposer: rx_headers,
        tx_consensus,
        tx_proposer: tx_parents,
        gc_round: 0,
        last_voted: HashMap::new(),
        processing: HashMap::new(),
        current_header: Header::default(),
        votes_aggregator: VotesAggregator::new(),
        certificates_aggregators: HashMap::new(),
        network: ReliableSender::new(),
        cancel_handlers: HashMap::new(),
    };

    // A valid conflicting header is suspended before voting: its payload is missing.
    core.sanitize_header(&waiting_header).unwrap();
    core.process_header(&waiting_header).await.unwrap();
    let resumed_header = match rx_sync_headers.recv().await.unwrap() {
        crate::header_waiter::WaiterMessage::SyncBatches(missing, h) => {
            assert_eq!(missing.len(), 1);
            h
        }
        message => panic!("Unexpected waiter request: {:?}", message),
    };
    assert!(!core.last_voted.contains_key(&1));

    let address = committee.primary(&author).unwrap().primary_to_primary;
    let tcp_listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (tx_votes, mut rx_votes) = channel(2);
    let vote_listener = tokio::spawn(async move {
        let (stream, _) = tcp_listener.accept().await.unwrap();
        let mut connection = tokio_util::codec::Framed::new(stream,
            tokio_util::codec::LengthDelimitedCodec::new());
        while let Some(Ok(bytes)) = connection.next().await {
            connection.send(Bytes::from("Ack")).await.unwrap();
            tx_votes.send(bytes.freeze()).await.unwrap();
        }
    });
    core.sanitize_header(&first_header).unwrap();
    core.process_header(&first_header).await.unwrap();
    let first_vote = match bincode::deserialize(&rx_votes.recv().await.unwrap()).unwrap() {
        PrimaryMessage::Vote(vote) => vote,
        message => panic!("Unexpected message: {:?}", message),
    };
    first_vote.verify(&committee).unwrap();
    assert_eq!(first_vote.id, first_header.id);
    assert_eq!(first_vote.author, name);
    assert!(core.last_voted[&1].contains(&author));

    // An unrelated message drives the real run-loop cleanup after feedback advances.
    // Its vote cannot match current_header, so this step has no header or vote effect.
    consensus_round.store(52, Ordering::Relaxed);
    tx_primary.send(PrimaryMessage::Vote(votes(&first_header).pop().unwrap())).await.unwrap();
    {
        let mut run = Box::pin(core.run());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(run.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        }).await;
    }
    assert_eq!(core.gc_round, 2);
    assert!(!core.last_voted.contains_key(&1));
    assert!(matches!(core.sanitize_header(&resumed_header), Err(DagError::TooOld(..))));

    // Dependency availability resumes the previously authenticated waiter input.
    let batch_key = [Digest([7; 32]).as_ref(), &0u32.to_le_bytes()].concat();
    store.write(batch_key.clone(), vec![1]).await;
    assert!(store.read(batch_key).await.unwrap().is_some());
    tx_loopback.send(resumed_header.clone()).await.unwrap();
    // Exercise the exact loopback dispatch before run-loop cleanup can cancel
    // the outbound message. Absence at the network alone does not prove no vote.
    let loopback = core.rx_header_waiter.recv().await.unwrap();
    core.process_header(&loopback).await.unwrap();
    let second_message = tokio::time::timeout(std::time::Duration::from_millis(500), rx_votes.recv()).await;
    vote_listener.abort();
    if let Ok(Some(bytes)) = &second_message {
        if let PrimaryMessage::Vote(vote) = bincode::deserialize(bytes).unwrap() {
            vote.verify(&committee).unwrap();
            assert_eq!(vote.id, resumed_header.id);
            assert_eq!(vote.author, name);
            println!("repeated_creator_round_vote first={:?} second={:?} round={} gc_round={}",
                first_vote.id, vote.id, vote.round, core.gc_round);
        }
    }
    assert!(second_message.is_err(), "stale waiter loopback voted again after last_voted GC");
    assert!(!core.last_voted.contains_key(&1));
    // Suppressing a stale vote must still allow the authenticated metadata path.
    assert!(store.read(resumed_header.id.to_vec()).await.unwrap().is_some());
}

#[tokio::test]
async fn process_header() {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let (name, secret) = keys.pop().unwrap();
    let mut signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_000);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make the vote we expect to receive.
    let expected = Vote::new(&header(), &name, &mut signature_service).await;

    // Spawn a listener to receive the vote.
    let address = committee
        .primary(&header().author)
        .unwrap()
        .primary_to_primary;
    let handle = listener(address);

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    tx_primary_messages
        .send(PrimaryMessage::Header(header()))
        .await
        .unwrap();

    // Ensure the listener correctly received the vote.
    let received = handle.await.unwrap();
    match bincode::deserialize(&received).unwrap() {
        PrimaryMessage::Vote(x) => assert_eq!(x, expected),
        x => panic!("Unexpected message: {:?}", x),
    }

    // Ensure the header is correctly stored.
    let stored = store
        .read(header().id.to_vec())
        .await
        .unwrap()
        .map(|x| bincode::deserialize(&x).unwrap());
    assert_eq!(stored, Some(header()));
}

#[tokio::test]
async fn process_header_missing_parent() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_parent";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    let header = Header {
        parents: [Digest::default()].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_header_missing_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send a header to the core.
    let header = Header {
        payload: [(Digest::default(), 0)].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_votes() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_100);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_vote";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Make the certificate we expect to receive.
    let expected = certificate(&Header::default());

    // Spawn all listeners to receive our newly formed certificate.
    let handles: Vec<_> = committee
        .others_primaries(&name)
        .iter()
        .map(|(_, address)| listener(address.primary_to_primary))
        .collect();

    // Send a votes to the core.
    for vote in votes(&Header::default()) {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    // Ensure all listeners got the certificate.
    for received in try_join_all(handles).await.unwrap() {
        match bincode::deserialize(&received).unwrap() {
            PrimaryMessage::Certificate(x) => assert_eq!(x, expected),
            x => panic!("Unexpected message: {:?}", x),
        }
    }
}

#[tokio::test]
async fn process_certificates() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(3);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(3);
    let (tx_parents, mut rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_certificates";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
    );

    // Send enough certificates to the core.
    let certificates: Vec<_> = headers()
        .iter()
        .take(3)
        .map(|header| certificate(header))
        .collect();

    for x in certificates.clone() {
        tx_primary_messages
            .send(PrimaryMessage::Certificate(x))
            .await
            .unwrap();
    }

    // Ensure the core sends the parents of the certificates to the proposer.
    let received = rx_parents.recv().await.unwrap();
    let parents = certificates.iter().map(|x| x.digest()).collect();
    assert_eq!(received, (parents, 1));

    // Ensure the core sends the certificates to the consensus.
    for x in certificates.clone() {
        let received = rx_consensus.recv().await.unwrap();
        assert_eq!(received, x);
    }

    // Ensure the certificates are stored.
    for x in &certificates {
        let stored = store.read(x.digest().to_vec()).await.unwrap();
        let serialized = bincode::serialize(x).unwrap();
        assert_eq!(stored, Some(serialized));
    }
}
