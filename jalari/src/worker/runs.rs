use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::job::{Command, RunControl};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Hold {
    pub(super) job_id: i64,
    pub(super) worker_id: Arc<str>,
    pub(super) token: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Owned {
    pub(super) job_id: i64,
    pub(super) token: i64,
    pub(super) command: Option<Command>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Verdict {
    pub(super) lost: Vec<i64>,
    pub(super) commands: Vec<(i64, Command)>,
    pub(super) orphans: Vec<Hold>,
    pub(super) abandoned: Vec<Hold>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Running,
    Closing,
    Retired,
    Abandoned,
}

struct Entry {
    hold: Hold,
    control: Arc<RunControl>,
    phase: Phase,
}

#[derive(Default)]
struct Book {
    runs: HashMap<i64, Entry>,
    pending: HashMap<i64, Arc<str>>,
}

#[derive(Default)]
pub(super) struct RunBook {
    book: Mutex<Book>,
    tokens: AtomicI64,
}

impl RunBook {
    pub(super) fn pending(&self, worker_id: &Arc<str>) -> PendingClaim<'_> {
        let token = self.tokens.fetch_add(1, Ordering::Relaxed) + 1;
        self.book
            .lock()
            .unwrap()
            .pending
            .insert(token, Arc::clone(worker_id));
        PendingClaim { book: self, token }
    }

    pub(super) fn running(&self, worker_id: &str) -> HashSet<i64> {
        self.book
            .lock()
            .unwrap()
            .runs
            .values()
            .filter(|entry| &*entry.hold.worker_id == worker_id && entry.phase == Phase::Running)
            .map(|entry| entry.hold.token)
            .collect()
    }

    pub(super) fn deliver(
        &self,
        worker_id: &Arc<str>,
        sent: &HashSet<i64>,
        owned: &[Owned],
    ) -> (Vec<Hold>, Vec<Hold>) {
        let mut book = self.book.lock().unwrap();
        book.runs.retain(|_, entry| {
            entry.phase != Phase::Abandoned || entry.hold.worker_id == *worker_id
        });
        let known: HashSet<i64> = book
            .pending
            .iter()
            .filter(|(_, owner)| *owner == worker_id)
            .map(|(token, _)| *token)
            .chain(
                book.runs
                    .values()
                    .filter(|entry| {
                        entry.hold.worker_id == *worker_id
                            && matches!(entry.phase, Phase::Running | Phase::Closing)
                    })
                    .map(|entry| entry.hold.token),
            )
            .collect();
        let abandoned: HashSet<i64> = book
            .runs
            .values()
            .filter(|entry| entry.phase == Phase::Abandoned)
            .map(|entry| entry.hold.token)
            .collect();
        let verdict = reconcile(worker_id, sent, &known, &abandoned, owned);
        for token in &verdict.lost {
            if let Some(entry) = book.runs.get(token) {
                entry.control.lost().cancel();
            }
        }
        for (token, command) in &verdict.commands {
            if let Some(entry) = book.runs.get(token) {
                entry.control.request(*command);
            }
        }
        (verdict.orphans, verdict.abandoned)
    }

    pub(super) fn forget(&self, holds: &[Hold]) {
        let mut book = self.book.lock().unwrap();
        for hold in holds {
            book.runs.remove(&hold.token);
        }
    }

    pub(super) fn live(&self) -> Vec<Hold> {
        let mut holds: Vec<Hold> = self
            .book
            .lock()
            .unwrap()
            .runs
            .values()
            .filter(|entry| matches!(entry.phase, Phase::Running | Phase::Closing))
            .map(|entry| entry.hold.clone())
            .collect();
        holds.sort_by_key(|hold| hold.job_id);
        holds
    }

    pub(super) fn dirty(&self) -> Vec<(Arc<RunControl>, Value)> {
        let book = self.book.lock().unwrap();
        let mut dirty: Vec<(Arc<RunControl>, Value)> = book
            .runs
            .values()
            .filter_map(|entry| {
                let progress = entry.control.take_dirty()?;
                Some((Arc::clone(&entry.control), progress))
            })
            .collect();
        dirty.sort_by_key(|(control, _)| control.job_id());
        dirty
    }

    pub(super) fn prune(&self) {
        self.book
            .lock()
            .unwrap()
            .runs
            .retain(|_, entry| entry.phase != Phase::Retired || entry.control.is_dirty());
    }

    fn retire(&self, token: i64, ending: Option<Phase>) {
        let mut book = self.book.lock().unwrap();
        match ending {
            None => {
                book.runs.remove(&token);
            }
            Some(phase) => {
                if let Some(entry) = book.runs.get_mut(&token) {
                    entry.phase = phase;
                }
            }
        }
    }
}

pub(super) fn reconcile(
    worker_id: &Arc<str>,
    sent: &HashSet<i64>,
    known: &HashSet<i64>,
    abandoned: &HashSet<i64>,
    owned: &[Owned],
) -> Verdict {
    let held: HashSet<i64> = owned.iter().map(|row| row.token).collect();
    let mut lost: Vec<i64> = sent.difference(&held).copied().collect();
    lost.sort_unstable();
    let mut verdict = Verdict {
        lost,
        ..Verdict::default()
    };
    for row in owned {
        let hold = || Hold {
            job_id: row.job_id,
            worker_id: Arc::clone(worker_id),
            token: row.token,
        };
        if known.contains(&row.token) {
            if let Some(command) = row.command {
                verdict.commands.push((row.token, command));
            }
        } else if abandoned.contains(&row.token) {
            verdict.abandoned.push(hold());
        } else {
            verdict.orphans.push(hold());
        }
    }
    verdict.orphans.sort_by_key(|hold| hold.job_id);
    verdict.abandoned.sort_by_key(|hold| hold.job_id);
    verdict
}

pub(super) struct PendingClaim<'a> {
    book: &'a RunBook,
    token: i64,
}

impl<'a> PendingClaim<'a> {
    pub(super) fn token(&self) -> i64 {
        self.token
    }

    pub(super) fn register(self, hold: Hold, control: Arc<RunControl>) -> Registration<'a> {
        let mut book = self.book.book.lock().unwrap();
        book.pending.remove(&self.token);
        book.runs.insert(
            self.token,
            Entry {
                hold,
                control,
                phase: Phase::Running,
            },
        );
        Registration {
            book: self.book,
            token: self.token,
            ending: Some(Phase::Retired),
        }
    }
}

impl Drop for PendingClaim<'_> {
    fn drop(&mut self) {
        self.book.book.lock().unwrap().pending.remove(&self.token);
    }
}

pub(super) struct Registration<'a> {
    book: &'a RunBook,
    token: i64,
    ending: Option<Phase>,
}

impl Registration<'_> {
    pub(super) fn closing(&self) {
        if let Some(entry) = self.book.book.lock().unwrap().runs.get_mut(&self.token) {
            entry.phase = Phase::Closing;
        }
    }

    pub(super) fn close(mut self) {
        self.ending = None;
    }

    pub(super) fn abandon(mut self) {
        self.ending = Some(Phase::Abandoned);
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.book.retire(self.token, self.ending);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(job_id: i64, token: i64, command: Option<Command>) -> Owned {
        Owned {
            job_id,
            token,
            command,
        }
    }

    fn tokens(values: &[i64]) -> HashSet<i64> {
        values.iter().copied().collect()
    }

    #[test]
    fn test_run_missing_from_the_beat_is_lost() {
        let worker: Arc<str> = Arc::from("w");
        let verdict = reconcile(
            &worker,
            &tokens(&[1, 2]),
            &tokens(&[1, 2]),
            &tokens(&[]),
            &[owned(10, 1, None)],
        );
        assert_eq!(verdict.lost, [2]);
        assert!(verdict.orphans.is_empty());
        assert!(verdict.commands.is_empty());
    }

    #[test]
    fn test_run_registered_after_the_beat_was_sent_is_not_judged() {
        let worker: Arc<str> = Arc::from("w");
        let verdict = reconcile(&worker, &tokens(&[]), &tokens(&[3]), &tokens(&[]), &[]);
        assert_eq!(verdict, Verdict::default());
    }

    #[test]
    fn test_owned_row_with_an_unknown_token_is_an_orphan() {
        let worker: Arc<str> = Arc::from("w");
        let verdict = reconcile(
            &worker,
            &tokens(&[1]),
            &tokens(&[1]),
            &tokens(&[]),
            &[owned(10, 1, None), owned(11, 7, Some(Command::Stop))],
        );
        assert_eq!(
            verdict.orphans,
            [Hold {
                job_id: 11,
                worker_id: Arc::clone(&worker),
                token: 7,
            }]
        );
        assert!(verdict.abandoned.is_empty());
        assert!(verdict.lost.is_empty());
        assert!(verdict.commands.is_empty());
    }

    #[test]
    fn test_owned_row_of_an_abandoned_run_is_not_an_orphan() {
        let worker: Arc<str> = Arc::from("w");
        let verdict = reconcile(
            &worker,
            &tokens(&[]),
            &tokens(&[]),
            &tokens(&[4]),
            &[owned(10, 4, None), owned(11, 9, None)],
        );
        assert_eq!(
            verdict.abandoned,
            [Hold {
                job_id: 10,
                worker_id: Arc::clone(&worker),
                token: 4,
            }]
        );
        assert_eq!(
            verdict.orphans,
            [Hold {
                job_id: 11,
                worker_id: Arc::clone(&worker),
                token: 9,
            }]
        );
    }

    #[test]
    fn test_pending_claim_is_neither_orphan_nor_lost() {
        let worker: Arc<str> = Arc::from("w");
        let verdict = reconcile(
            &worker,
            &tokens(&[]),
            &tokens(&[5]),
            &tokens(&[]),
            &[owned(12, 5, None)],
        );
        assert_eq!(verdict, Verdict::default());
    }

    #[test]
    fn test_commands_reach_known_runs() {
        let worker: Arc<str> = Arc::from("w");
        let verdict = reconcile(
            &worker,
            &tokens(&[1, 2]),
            &tokens(&[1, 2]),
            &tokens(&[]),
            &[owned(10, 1, Some(Command::Cancel)), owned(11, 2, None)],
        );
        assert_eq!(verdict.commands, [(1, Command::Cancel)]);
        assert!(verdict.lost.is_empty());
        assert!(verdict.orphans.is_empty());
    }
}
