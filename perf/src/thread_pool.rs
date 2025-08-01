use std::{
    fmt,
    hash::{BuildHasher, Hash, Hasher as _},
    panic::{catch_unwind, AssertUnwindSafe},
};

/// Creates a scoped thread pool with per-thread mutable state.
///
/// All threads and jobs will be joined before this function returns.
pub fn scope<'scope, N, T, F, S>(
    name: N,
    num_workers: usize,
    inputs: &'scope [T],
    random_state: S,
    f: F,
) where
    N: fmt::Display,
    T: Hash + Send + Sync + 'scope,
    S: BuildHasher,
    F: for<'call> Fn(&'call [&'call T]) + Send + Sync + 'scope,
{
    let mut partitioned_inputs: Vec<_> = (0..num_workers)
        .map(|_| Vec::with_capacity(inputs.len().div_ceil(num_workers)))
        .collect();

    for input in inputs {
        let mut hasher = random_state.build_hasher();
        input.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        partitioned_inputs[hash % num_workers].push(input);
    }

    let f = &f;

    std::thread::scope(|s| {
        for (i, inputs) in partitioned_inputs.into_iter().enumerate() {
            std::thread::Builder::new()
                .name(format!("{name}{i:02}"))
                .spawn_scoped(s, move || {
                    let _ = catch_unwind(AssertUnwindSafe(|| f(inputs.as_slice())));
                })
                .unwrap();
        }
    })
}

#[cfg(test)]
mod test {
    use {
        super::*,
        std::{
            hash::RandomState,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
        },
    };

    /// Checks whether worker states are actually modified by jobs.
    #[test]
    fn test_thread_pool() {
        let counter = Arc::new(AtomicUsize::default());
        let inputs: Vec<_> = (0..1000).collect();
        scope(
            "foo",
            num_cpus::get(),
            &inputs,
            RandomState::new(),
            |inputs| {
                let counter = Arc::clone(&counter);
                for input in inputs {
                    counter.fetch_add(**input, Ordering::Relaxed);
                }
            },
        );

        let expected: usize = (0..1000).sum();
        assert_eq!(counter.load(Ordering::Relaxed), expected);
    }

    /// Checks whether scoped tasks can use immutable references.
    #[test]
    #[ignore]
    fn test_thread_pool_scope_ref() {
        let jobs = num_cpus::get();
        let v: Vec<_> = (0..10_000).collect();

        scope("foo", jobs, &[1], RandomState::new(), |inputs| {
            // for i in v.iter() {
            //     s.spawn(|state| state.push(*i));
            // }
        });
    }

    /// Checks whether scoped tasks can use mutable references.
    #[test]
    #[ignore]
    fn test_thread_pool_scope_ref_mut() {
        let mut v: Vec<u32> = (0..100).collect();

        scope("foo", num_cpus::get(), &[1], RandomState::new(), |inputs| {
            // for i in v.iter_mut() {
            //     s.spawn(|_| *i = i.saturating_add(1));
            // }
        });

        assert_eq!(v, (1..=100).collect::<Vec<u32>>());
    }

    /// Check whether scoped tasks can use moved values.
    #[test]
    #[ignore]
    fn test_thread_pool_scope_move() {
        let value: u32 = 5;

        scope("foo", num_cpus::get(), &[1], RandomState::new(), |inputs| {
            // for _ in 0..10 {
            //     s.spawn(move |state| {
            //         *state = state.saturating_add(value);
            //     });
            // }
        });
    }
}
