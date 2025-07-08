use std::{
    cell::Cell,
    marker::PhantomData,
    mem,
    ops::Deref,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
};

#[derive(Default)]
pub struct ThreadPoolState {
    pending: AtomicUsize,
    join_lock: Mutex<()>,
    join_cvar: Condvar,
}

impl ThreadPoolState {
    #[inline]
    fn has_pending_jobs(&self) -> bool {
        self.pending.load(Ordering::Relaxed) > 0
    }

    #[inline]
    fn join(&self) {
        let mut lock = self.join_lock.lock().unwrap();
        while self.has_pending_jobs() {
            lock = self.join_cvar.wait(lock).unwrap();
        }
    }
}

type Job<'a> = Box<dyn FnOnce() + Send + 'a>;

pub struct Worker {
    thread: JoinHandle<()>,
}

impl Worker {
    fn new(
        name: String,
        receiver: Arc<Mutex<Receiver<Job<'static>>>>,
        pool_state: Arc<ThreadPoolState>,
    ) -> Self {
        let thread = thread::Builder::new()
            .name(name)
            .spawn(move || {
                while let Ok(job) = receiver.lock().unwrap().recv() {
                    // Don't let the worker thread panic.
                    // TODO: But would be nice to propagate that error.
                    let _ = catch_unwind(AssertUnwindSafe(job));

                    pool_state.pending.fetch_sub(1, Ordering::Relaxed);
                    if !pool_state.has_pending_jobs() {
                        let _guard = pool_state.join_lock.lock().unwrap();
                        pool_state.join_cvar.notify_one();
                    }
                }
            })
            .unwrap();
        Self { thread }
    }
}

impl Deref for Worker {
    type Target = JoinHandle<()>;

    fn deref(&self) -> &Self::Target {
        &self.thread
    }
}

/// A simple thread pool implementation.
pub struct ThreadPool {
    #[allow(dead_code)]
    workers: Vec<Worker>,
    tx: mpsc::Sender<Job<'static>>,
    pool_state: Arc<ThreadPoolState>,
}

impl ThreadPool {
    pub fn new(name: impl AsRef<str>, size: usize) -> Self {
        let name = name.as_ref();

        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let pool_state = Arc::new(ThreadPoolState::default());

        let workers: Vec<_> = (0..size)
            .map(|i| {
                Worker::new(
                    format!("{name}{i}"),
                    Arc::clone(&rx),
                    Arc::clone(&pool_state),
                )
            })
            .collect();

        Self {
            workers,
            tx,
            pool_state,
        }
    }

    pub fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx.send(Box::new(f)).unwrap();

        self.pool_state.pending.fetch_add(1, Ordering::Relaxed);
    }

    pub fn join(&self) {
        if !self.pool_state.has_pending_jobs() {
            return;
        }
        self.pool_state.join();
    }

    pub fn scope<'pool, 'scope, F, R>(&'pool self, f: F) -> R
    where
        F: FnOnce(&Scope<'pool, 'scope>) -> R,
    {
        let scope = Scope {
            pool: self,
            _marker: PhantomData,
        };
        f(&scope)
    }
}

pub struct Scope<'pool, 'scope> {
    pool: &'pool ThreadPool,
    _marker: PhantomData<Cell<&'scope mut ()>>,
}

impl<'scope> Scope<'_, 'scope> {
    pub fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'scope,
    {
        // SAFETY: `ThreadPool` runs jobs with 'static lifetime. We guarantee
        // that the lifetime of a scoped job doesn't exceed 'scope by calling
        // `join` in the `Drop` implementation.
        let f = unsafe { mem::transmute::<Job<'scope>, Job<'static>>(Box::new(f)) };
        self.pool.spawn(f);
    }
}

impl Drop for Scope<'_, '_> {
    fn drop(&mut self) {
        self.pool.join();
    }
}

#[cfg(test)]
mod test {
    use std::{thread::sleep, time::Duration};

    use super::*;

    /// Checks whether sending simple computation results through a mpsc
    /// channel works.
    #[test]
    fn test_thread_pool_channel() {
        let thread_pool = ThreadPool::new("foo", num_cpus::get());
        let (tx, rx) = mpsc::channel();

        for i in 0..32 {
            let tx = tx.clone();
            thread_pool.spawn(move || {
                tx.send(i * i).unwrap();
            });
        }

        thread_pool.join();
        let mut res: Vec<_> = rx.iter().take(32).collect();
        res.sort();
        assert_eq!(
            res,
            [
                0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121, 144, 169, 196, 225, 256, 289, 324,
                361, 400, 441, 484, 529, 576, 625, 676, 729, 784, 841, 900, 961
            ]
        );

        for i in 32..64 {
            let tx = tx.clone();
            thread_pool.spawn(move || {
                tx.send(i * i).unwrap();
            });
        }

        thread_pool.join();
        let mut res: Vec<_> = rx.iter().take(32).collect();
        res.sort();
        assert_eq!(
            res,
            [
                1024, 1089, 1156, 1225, 1296, 1369, 1444, 1521, 1600, 1681, 1764, 1849, 1936, 2025,
                2116, 2209, 2304, 2401, 2500, 2601, 2704, 2809, 2916, 3025, 3136, 3249, 3364, 3481,
                3600, 3721, 3844, 3969
            ]
        );
    }

    /// Checks whether scoped tasks can use immutable references.
    #[test]
    fn test_thread_pool_scope_channel() {
        let thread_pool = ThreadPool::new("scoped", num_cpus::get());
        let (tx, rx) = mpsc::channel();
        let v = vec![0, 1, 2, 3, 4];
        thread_pool.scope(|s| {
            for i in v.iter() {
                let tx = tx.clone();
                s.spawn(move || tx.send(*i).unwrap());
            }
        });
        assert!(!thread_pool.pool_state.has_pending_jobs());
        let res: Vec<_> = rx.iter().take(v.len()).collect();
        assert_eq!(v, res);
    }

    #[test]
    fn test_thread_pool_scope_channel_dropped() {
        let thread_pool = ThreadPool::new("scoped", num_cpus::get());
        let (tx, rx) = mpsc::channel();
        let v = vec![0, 1, 2, 3, 4];
        thread_pool.scope(|s| {
            for i in v.iter() {
                let tx = tx.clone();
                s.spawn(move || tx.send(*i).unwrap());
            }
        });
        drop(tx);
        assert!(!thread_pool.pool_state.has_pending_jobs());
        let res: Vec<_> = rx.iter().collect();
        assert_eq!(v, res);
    }

    /// Checks whether scoped tasks can use mutable references.
    #[test]
    fn test_thread_pool_scope_mut() {
        let thread_pool = ThreadPool::new("scoped", num_cpus::get());
        let mut v: Vec<u32> = vec![0, 1, 2, 3, 4];
        thread_pool.scope(|s| {
            for i in v.iter_mut() {
                s.spawn(|| *i = i.saturating_add(1));
            }
        });
        assert_eq!(v, vec![1, 2, 3, 4, 5]);
    }

    /// Ensures that all scoped tasks are done before dropping the scope.
    #[test]
    fn test_thread_pool_scope_lifetime() {
        let num_jobs = num_cpus::get();
        let threadpool = ThreadPool::new("foo", num_jobs);

        threadpool.scope(|s| {
            for _ in 0..num_jobs {
                s.spawn(|| sleep(Duration::from_millis(10)));
            }

            assert_eq!(
                threadpool.pool_state.pending.load(Ordering::Relaxed),
                num_jobs
            );
        });
        assert_eq!(threadpool.pool_state.pending.load(Ordering::Relaxed), 0);
    }
}
