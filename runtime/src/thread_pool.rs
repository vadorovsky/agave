use {
    crossbeam_deque::{Injector, Steal, Stealer, Worker},
    std::{
        fmt,
        panic::{catch_unwind, AssertUnwindSafe},
        sync::Arc,
    },
};

type Job<'a, T> = Box<dyn FnOnce(&'a mut T) + Send + 'a>;

pub struct ScopedThreadPool<'scope, T> {
    injector: Arc<Injector<Job<'scope, T>>>,
}

impl<'scope, T> ScopedThreadPool<'scope, T>
where
    T: 'scope,
{
    pub fn spawn<F>(&self, f: F)
    where
        F: FnOnce(&'scope mut T) + Send + 'scope,
    {
        let job = Box::new(f);
        self.injector.push(job);
    }
}

fn run_worker<'scope, T>(
    local: &Worker<Job<'scope, T>>,
    global: &Injector<Job<'scope, T>>,
    stealers: &[Stealer<Job<'scope, T>>],
    worker_state: &'scope mut T,
) {
    loop {
        // Try local queue first.
        let job = match local.pop() {
            Some(job) => job,
            None => {
                // Then try stealing.
                let steal = global
                    .steal_batch_and_pop(local)
                    .or_else(|| stealers.iter().map(|s| s.steal()).collect());
                match steal {
                    Steal::Success(job) => job,
                    Steal::Retry => continue,
                    Steal::Empty => break,
                }
            }
        };

        // lose the lifetime
        let worker_state = unsafe { &mut *(worker_state as *mut T) };
        let _ = catch_unwind(AssertUnwindSafe(|| job(worker_state)));
    }
}

/// Creates a scoped thread pool with per-thread mutable state.
///
/// All threads and jobs will be joined before this function returns.
pub fn scope_with_states<'scope, N, T, F, R>(name: N, worker_states: &'scope mut [T], f: F)
where
    N: fmt::Display,
    T: Send + 'scope,
    F: FnOnce(&ScopedThreadPool<'scope, T>) -> R,
{
    let injector = Arc::new(Injector::new());
    let pool = ScopedThreadPool {
        injector: Arc::clone(&injector),
    };

    std::thread::scope(|s| {
        let workers: Vec<_> = (0..worker_states.len())
            .map(|_| Worker::new_lifo())
            .collect();
        let stealers: Vec<_> = workers.iter().map(Worker::stealer).collect();
        for (i, (worker_state, worker)) in worker_states.iter_mut().zip(workers).enumerate() {
            let injector = injector.clone();
            let stealers = stealers.clone();
            std::thread::Builder::new()
                .name(format!("{name}{i:02}"))
                .spawn_scoped(s, move || {
                    run_worker(&worker, &injector, &stealers, worker_state)
                })
                .unwrap();
        }

        f(&pool);
    });
}

#[cfg(test)]
mod test {
    use super::*;

    /// Checks whether scoped tasks can use immutable references.
    #[test]
    fn test_thread_pool_scope_ref() {
        let mut states: Vec<Vec<u32>> = (0..num_cpus::get())
            .map(|_| Vec::with_capacity(32))
            .collect();
        let v: Vec<_> = (0..10_000).collect();

        scope_with_states("foo", &mut states, |s| {
            for i in v.iter() {
                s.spawn(|state| state.push(*i));
            }
        });

        let mut res: Vec<u32> = states.into_iter().flatten().collect();
        res.sort();
        assert_eq!(v, res);
    }

    /// Checks whether scoped tasks can use mutable references.
    #[test]
    fn test_thread_pool_scope_ref_mut() {
        let mut states: Vec<()> = (0..num_cpus::get()).map(|_| ()).collect();
        let mut v: Vec<u32> = (0..10_000).collect();

        scope_with_states("foo", &mut states, |s| {
            for i in v.iter_mut() {
                s.spawn(|_| *i = i.saturating_add(1));
            }
        });

        assert_eq!(v, (1..=10_000).collect::<Vec<u32>>());
    }

    /// Tests nested pool usage.
    #[test]
    fn test_nested_pools() {
        let mut outer_states = vec![0; num_cpus::get()];
        let mut inner_states = vec![0; num_cpus::get()];

        scope_with_states("outer", &mut outer_states, |outer_pool| {
            outer_pool.spawn(|_| {
                scope_with_states("inner", &mut inner_states, |inner_pool| {
                    for i in 0..100 {
                        inner_pool.spawn(move |state| *state += i);
                    }
                });
            });
        });

        let inner_sum: i32 = inner_states.iter().sum();
        assert_eq!(inner_sum, (0..100).sum::<i32>());
    }
}
