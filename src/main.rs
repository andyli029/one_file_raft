#![feature(map_try_insert)]

use one_file_raft::{Event, Log, Net, Raft, Store};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(log::LevelFilter::Debug).init();

    let sto = || Store::new(vec![[1, 2, 3].into_iter().collect()]);

    let (tx1, rx1) = mpsc::unbounded_channel();
    let (tx2, rx2) = mpsc::unbounded_channel();
    let (tx3, rx3) = mpsc::unbounded_channel();

    let net = || {
        let mut r = Net::default();
        r.targets.insert(1, tx1.clone());
        r.targets.insert(2, tx2.clone());
        r.targets.insert(3, tx3.clone());
        r
    };

    let n1 = Raft::new(1, sto(), net(), rx1);
    let n2 = Raft::new(2, sto(), net(), rx2);
    let n3 = Raft::new(3, sto(), net(), rx3);

    let mut metrics1 = n1.metrics.subscribe();

    let h1 = tokio::spawn(n1.run());
    let h2 = tokio::spawn(n2.run());
    let h3 = tokio::spawn(n3.run());

    tx1.send((0, Event::Elect)).unwrap();

    while metrics1.borrow().vote.committed.is_none() {
        metrics1.changed().await.unwrap();
    }

    log::info!("=== Leader Established");

    tx1.send((
        0,
        Event::Write(Log { data: Some(s("x=3")), ..Default::default() }),
    ))
    .unwrap();

    h1.await;
    h2.await;
    h3.await;
}

fn s(x: impl ToString) -> String {
    x.to_string()
}
