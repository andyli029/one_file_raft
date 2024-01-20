//! Features:
//!
//! - [x] Election(`Raft::elect()`)
//! - [x] Log replication(`Raft::handle_replicate_req()`)
//! - [x] Commit
//! - [x] Write application data(`Raft::write()`)
//! - [x] Membership store(`Store::configs`).
//! - [ ] Membership change: joint consensus.
//! - [x] Non-async event loop model(main loop: `Raft::run()`).
//! - [x] Pseudo network simulated by mpsc channels(`Net`).
//! - [x] Pseudo Log store simulated by in-memory store(`Store`).
//!
//! Not yet implemented:
//! - [ ] State machine(`Raft::commit()` is a no-op entry)
//! - [ ] Log compaction
//! - [ ] Log purge
//!
//! Implementation details:
//! - [x] Membership config takes effect once appended(not on-applied).
//! - [x] Standalone Leader, it has to check vote when accessing local store.
//! - [x] Leader access store directly(not via RPC).
//! - [ ] Append log when vote?

#![feature(map_try_insert)]

use std::cmp::{max, min, Ordering};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use derive_more::Display;
use derive_new::new as New;
use itertools::Itertools;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, Hash, Display)]
#[display(fmt = "L({})", _0)]
pub struct LeaderId(pub u64);

impl PartialOrd for LeaderId {
    fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
        [None, Some(Ordering::Equal)][(self.0 == b.0) as usize]
    }
}

#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, PartialOrd, Hash)]
#[derive(Display, New)]
#[display(fmt = "T{}-{:?}-{}", term, committed, leader_id)]
pub struct Vote {
    pub term: u64,
    pub committed: Option<()>,
    pub leader_id: LeaderId,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Display)]
#[display(fmt = "T{}-{}", term, index)]
pub struct LogId {
    term: u64,
    index: u64,
}

#[derive(Debug, Clone, Default, Display)]
#[display(fmt = "Log({}, {:?}, {:?})", log_id, data, config)]
pub struct Log {
    pub log_id: LogId,
    pub data: Option<String>,
    pub config: Option<Vec<BTreeSet<u64>>>,
}

#[derive(Debug, Default)]
pub struct Net {
    pub targets: BTreeMap<u64, mpsc::UnboundedSender<(u64, Event)>>,
}

impl Net {
    fn send(&mut self, from: u64, target: u64, ev: Event) {
        log::debug!("N{} send --> N{} {}", from, target, ev);
        let tx = self.targets.get(&target).unwrap();
        tx.send((from, ev)).unwrap();
    }
}

#[derive(Debug)]
pub struct Request {
    vote: Vote,
    last_log_id: LogId,

    prev: LogId,
    logs: Vec<Log>,
    // TODO: commit index
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "vote:{} last:{}", self.vote, self.last_log_id,)?;
        write!(f, " logs:{}|{}", self.prev, self.logs.iter().join(","))
    }
}

#[derive(Debug, Display)]
#[display(fmt = "granted:{} vote:{} {:?}", granted, vote, log)]
pub struct Reply {
    granted: bool,
    vote: Vote,
    log: Result<LogId, u64>,
}

#[derive(Debug, Display)]
pub enum Event {
    #[display(fmt = "Request({})", _0)]
    Request(Request),
    #[display(fmt = "Reply({})", _0)]
    Reply(Reply),
    Elect,
    #[display(fmt = "Write({})", _0)]
    Write(Log),
}

#[derive(Debug, Clone, Copy, Default, New)]
pub struct Progress {
    acked: LogId,
    len: u64,
    sending: bool,
}

pub struct Leading {
    vote: Vote,
    config: Vec<BTreeSet<u64>>,
    granted_by: BTreeSet<u64>,
    progresses: BTreeMap<u64, Progress>,
    log_index_range: (u64, u64),
}

#[derive(Debug, Default, Clone, PartialEq, Eq, derive_new::new)]
pub struct Metrics {
    pub vote: Vote,
    pub last_log: LogId,
    pub commit: u64,
}

#[derive(Debug, Default)]
pub struct Store {
    vote: Vote,
    configs: BTreeMap<u64, Vec<BTreeSet<u64>>>,
    logs: Vec<Log>,
}

impl Store {
    pub fn new(membership: Vec<BTreeSet<u64>>) -> Self {
        let mut configs = BTreeMap::new();
        configs.insert(0, membership);
        Store { vote: Vote::default(), configs, logs: vec![Log::default()] }
    }

    fn last(&self) -> LogId {
        self.logs.last().map(|x| x.log_id).unwrap_or_default()
    }

    fn truncate(&mut self, log_id: LogId) {
        log::debug!("Store::truncate: {}", log_id);
        self.configs.retain(|&x, _| x < log_id.index);
        self.logs.truncate(log_id.index as usize);
    }

    fn append(&mut self, logs: Vec<Log>) {
        log::debug!("append: {}", logs.iter().map(|x| x.to_string()).join(", "));
        for log in logs {
            if let Some(x) = self.get_log_id(log.log_id.index) {
                if x != log.log_id {
                    self.truncate(x);
                } else {
                    continue;
                }
            }
            if let Some(ref membership) = log.config {
                self.configs.insert(log.log_id.index, membership.clone());
            }
            self.logs.push(log);
        }
    }

    fn has_log_id(&self, log_id: LogId) -> bool {
        self.get_log_id(log_id.index).map(|x| x == log_id).unwrap_or(false)
    }

    fn get_log_id(&self, rel_index: u64) -> Option<LogId> {
        self.logs.get(rel_index as usize).map(|x| x.log_id)
    }

    fn read_logs(&self, index: u64, n: u64) -> Vec<Log> {
        log::debug!("read_logs: index={} n={}", index, n);
        let logs: Vec<_> = self.logs[index as usize..].iter().take(n as usize).cloned().collect();
        log::debug!("read_logs: ret: {}", logs.iter().join(", "));
        logs
    }

    fn check_vote(&mut self, vote: Vote) -> (bool, Vote) {
        log::debug!("check_vote: mine: {}, incoming: {}", self.vote, vote);
        if vote > self.vote {
            log::info!("update_vote: {} --> {}", self.vote, vote);
            self.vote = vote;
        }
        log::debug!("check_vote: ret: {}", self.vote);
        (vote == self.vote, self.vote)
    }
}

pub struct Raft {
    pub id: u64,
    pub leading: Option<Leading>,
    pub commit: u64,
    pub net: Net,
    pub store: Store,
    pub metrics: watch::Sender<Metrics>,
    pub rx: mpsc::UnboundedReceiver<(u64, Event)>,
}

impl Raft {
    pub fn new(id: u64, store: Store, net: Net, rx: mpsc::UnboundedReceiver<(u64, Event)>) -> Self {
        let (metrics, _) = watch::channel(Metrics::default());
        Raft { id, leading: None, commit: 0, net, store, metrics, rx }
    }

    #[logcall::logcall("info")]
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        loop {
            let m = Metrics::new(self.store.vote, self.store.last(), self.commit);
            self.metrics.send_replace(m);

            let (from, ev) = self.rx.recv().await.ok_or(anyhow::anyhow!("closed"))?;
            log::debug!("N{} recv <-- N{} {}", self.id, from, ev);
            match ev {
                Event::Request(req) => {
                    let reply = self.handle_replicate_req(req);
                    self.net.send(self.id, from, Event::Reply(reply));
                }
                Event::Reply(reply) => {
                    self.handle_replicate_reply(from, reply);
                }
                Event::Elect => {
                    self.elect();
                }
                Event::Write(log) => {
                    self.write(log);
                }
            }
        }
    }

    #[logcall::logcall("info")]
    fn write(&mut self, mut log: Log) -> Option<LogId> {
        let leader_vote = self.leading.as_mut()?.vote;
        leader_vote.committed?;

        if !self.store.check_vote(leader_vote).0 {
            self.leading = None;
            return None;
        }

        let l = self.leading.as_mut()?;

        let log_id = LogId { term: l.vote.term, index: l.log_index_range.1 };
        l.log_index_range.1 += 1;
        log.log_id = log_id;

        if let Some(ref membership) = log.config {
            let ids = node_ids(membership);
            l.progresses =
                ids.map(|x| (x, l.progresses.get(&x).copied().unwrap_or_default())).collect();
        }
        self.store.append(vec![log]);

        // Mark it as sending, so that it won't be sent again.
        l.progresses.insert(self.id, Progress::new(log_id, log_id.index, true));

        node_ids(&l.config).for_each(|id| self.send_if_idle(id, 10));
        Some(log_id)
    }

    fn elect(&mut self) {
        self.store.vote = Vote::new(self.store.vote.term + 1, None, LeaderId(self.id));

        let noop_index = self.store.last().index + 1;
        let config = self.store.configs.values().last().unwrap().clone();
        let ids = node_ids(&config);
        log::debug!("ids: {:?}", node_ids(&config).collect::<Vec<_>>());
        let progresses =
            ids.map(|id| (id, Progress::new(LogId::default(), noop_index, false))).collect();

        self.leading = Some(Leading {
            vote: self.store.vote,
            config: config.clone(),
            granted_by: BTreeSet::new(),
            progresses,
            log_index_range: (noop_index, noop_index),
        });

        node_ids(&config).for_each(|x| self.send_if_idle(x, 0));
    }

    fn handle_replicate_reply(&mut self, target: u64, reply: Reply) -> Option<Leading> {
        let leader_vote = self.leading.as_mut()?.vote;

        // TODO: is there a chance store.vote changed?
        if reply.granted && reply.vote == leader_vote {
            {
                let l = self.leading.as_mut()?;
                if l.vote.committed.is_none() {
                    l.granted_by.insert(target);

                    if granted_by_quorum(&l.config, &l.granted_by) {
                        l.vote.committed = Some(());
                        log::info!("Leader established: {}", l.vote);

                        self.net.send(self.id, self.id, Event::Write(Log::default()));
                    }
                }
            }

            let (acked, last_index) = {
                let l = self.leading.as_mut()?;
                let p = l.progresses.get_mut(&target).unwrap();
                assert!(p.sending);

                *p = match reply.log {
                    Ok(acked) => Progress::new(acked, max(p.len, acked.index + 1), false),
                    Err(end_index) => Progress::new(p.acked, min(p.len, end_index), false),
                };

                let (noop_index, len) = l.log_index_range;
                let acked = p.acked.index;
                if acked >= noop_index {
                    let granted = l.progresses.iter().filter(|(_id, p)| p.acked.index >= acked);
                    let this = granted.map(|(id, _)| *id).collect();
                    // TODO: possible a smaller index is committed
                    if granted_by_quorum(&l.config, &this) {
                        self.commit(acked);
                    }
                }

                (acked, len - 1)
            };

            if last_index > acked {
                self.send_if_idle(target, last_index - acked);
            }
        } else {
            self.store.check_vote(reply.vote);
        }
        None
    }

    fn handle_replicate_req(&mut self, req: Request) -> Reply {
        let my_last = self.store.last();
        let (is_granted, vote) = self.store.check_vote(req.vote);
        let is_upto_date = req.last_log_id >= my_last;

        let req_last = req.logs.last().map(|x| x.log_id).unwrap_or(req.prev);

        if is_granted && is_upto_date {
            let log = if self.store.has_log_id(req.prev) {
                self.store.append(req.logs);
                Ok(req_last)
            } else {
                self.store.truncate(req.prev);
                Err(req.prev.index)
            };

            Reply { granted: true, vote, log }
        } else {
            Reply { granted: false, vote, log: Err(my_last.index + 1) }
        }
    }

    fn send_if_idle(&mut self, target: u64, n: u64) {
        let l = self.leading.as_mut().unwrap();

        let prog = l.progresses.get_mut(&target).unwrap();
        if prog.sending {
            return;
        }
        prog.sending = true;

        let req = Request {
            vote: self.store.vote,
            last_log_id: self.store.last(),
            prev: prog.acked,
            logs: self.store.read_logs(prog.acked.index + 1, n),
        };

        self.net.send(self.id, target, Event::Request(req));
    }

    fn commit(&mut self, index: u64) {
        log::debug!("commit: {}: {}", index, self.store.logs[index as usize]);
        self.commit = index;
    }
}

pub fn granted_by_quorum(config: &Vec<BTreeSet<u64>>, granted_by: &BTreeSet<u64>) -> bool {
    for group in config {
        if group.intersection(granted_by).count() <= group.len() / 2 {
            return false;
        }
    }
    true
}

pub fn node_ids(config: &[BTreeSet<u64>]) -> impl Iterator<Item = u64> + 'static {
    config.iter().flat_map(|x| x.iter().copied()).collect::<BTreeSet<_>>().into_iter()
}
