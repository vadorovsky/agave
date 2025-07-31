use {
    itertools::izip,
    std::{
        fmt,
        hash::{BuildHasher, Hash, Hasher as _},
        panic::{catch_unwind, AssertUnwindSafe},
    },
};

/// Creates a scoped thread pool with per-thread mutable state.
///
/// All threads and jobs will be joined before this function returns.
pub fn scope_with_states<'scope, N, T, U, F, S>(
    name: N,
    worker_states: &'scope mut [T],
    inputs: &'scope [U],
    random_state: S,
    f: F,
) where
    N: fmt::Display,
    T: Send + 'scope,
    U: Hash + Send + Sync + 'scope,
    S: BuildHasher,
    F: for<'call> Fn(&'call mut T, &'call [&'call U]) + Send + Sync + 'scope,
{
    let num_workers = worker_states.len();

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
        for (i, (worker_state, inputs)) in
            izip!(worker_states, partitioned_inputs.into_iter()).enumerate()
        {
            std::thread::Builder::new()
                .name(format!("{name}{i:02}"))
                .spawn_scoped(s, move || {
                    let _ = catch_unwind(AssertUnwindSafe(|| f(worker_state, inputs.as_slice())));
                })
                .unwrap();
        }
    })
}

#[cfg(test)]
mod test {
    use {super::*, std::hash::RandomState};

    /// Checks whether worker states are actually modified by jobs.
    #[test]
    fn test_thread_pool_state() {
        let mut states: Vec<u32> = vec![u32::default(); num_cpus::get()];

        scope_with_states(
            "foo",
            &mut states,
            &[1; 10_000],
            RandomState::new(),
            |state, inputs| {
                for input in inputs {
                    *state = state.saturating_add(1);
                }
                // for _ in 0..10_000 {
                //     s.spawn(|state| *state = state.saturating_add(1));
                // }
            },
        );

        let sum: u32 = states.iter().sum();
        assert_eq!(sum, 10_000);
    }

    /// Checks whether scoped tasks can use immutable references.
    #[test]
    #[ignore]
    fn test_thread_pool_scope_ref() {
        let jobs = num_cpus::get();
        let mut states: Vec<Vec<u32>> = (0..jobs)
            .map(|_| Vec::with_capacity(10_000 / jobs))
            .collect();
        let v: Vec<_> = (0..10_000).collect();

        scope_with_states(
            "foo",
            &mut states,
            &[1],
            RandomState::new(),
            |state, inputs| {
                // for i in v.iter() {
                //     s.spawn(|state| state.push(*i));
                // }
            },
        );

        let mut res: Vec<u32> = states.into_iter().flatten().collect();
        res.sort();
        assert_eq!(v, res);
    }

    /// Checks whether scoped tasks can use mutable references.
    #[test]
    #[ignore]
    fn test_thread_pool_scope_ref_mut() {
        let mut states: Vec<()> = (0..num_cpus::get()).map(|_| ()).collect();
        let mut v: Vec<u32> = (0..100).collect();

        scope_with_states(
            "foo",
            &mut states,
            &[1],
            RandomState::new(),
            |state, inputs| {
                // for i in v.iter_mut() {
                //     s.spawn(|_| *i = i.saturating_add(1));
                // }
            },
        );

        assert_eq!(v, (1..=100).collect::<Vec<u32>>());
    }

    /// Check whether scoped tasks can use moved values.
    #[test]
    #[ignore]
    fn test_thread_pool_scope_move() {
        let value: u32 = 5;
        let mut states = vec![u32::default(); num_cpus::get()];

        scope_with_states(
            "foo",
            &mut states,
            &[1],
            RandomState::new(),
            |state, inputs| {
                // for _ in 0..10 {
                //     s.spawn(move |state| {
                //         *state = state.saturating_add(value);
                //     });
                // }
            },
        );

        let sum: u32 = states.iter().sum();
        assert_eq!(sum, 50);
    }
}
