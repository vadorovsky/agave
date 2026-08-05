use std::{cmp::Ordering, collections::VecDeque, fmt, ops::Range};

/// A double-ended queue which stores up to `N` elements inline and spills to a
/// heap-allocated [`VecDeque`] when it grows beyond that capacity.
///
/// Once spilled, the queue stays heap-allocated. This keeps the transition
/// simple and avoids repeatedly moving elements between representations.
pub(crate) enum SmallVecDeque<T, const N: usize> {
    Inline {
        entries: [Option<T>; N],
        head: u8,
        len: u8,
    },
    // Boxing the spilled representation keeps the common inline container
    // compact at the cost of one additional allocation when it spills.
    #[allow(clippy::box_collection)]
    Heap(Box<VecDeque<T>>),
}

impl<T, const N: usize> SmallVecDeque<T, N> {
    pub(crate) fn new() -> Self {
        assert!(N > 0, "inline capacity must be greater than zero");
        assert!(N <= u8::MAX.into(), "inline capacity must fit in a u8");
        Self::Inline {
            entries: std::array::from_fn(|_| None),
            head: 0,
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => usize::from(*len),
            Self::Heap(entries) => entries.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn inline_index(head: usize, logical_index: usize) -> usize {
        head.wrapping_add(logical_index)
            .checked_rem(N)
            .expect("nonzero inline capacity")
    }

    fn spill(&mut self) {
        let heap_capacity = N.saturating_mul(2).max(4);
        let old = std::mem::replace(
            self,
            Self::Heap(Box::new(VecDeque::with_capacity(heap_capacity))),
        );
        let Self::Inline {
            mut entries,
            head,
            len,
        } = old
        else {
            unreachable!("only the inline representation can spill");
        };
        let Self::Heap(heap) = self else {
            unreachable!();
        };
        for logical_index in 0..usize::from(len) {
            let physical_index = Self::inline_index(usize::from(head), logical_index);
            let entry = entries
                .get_mut(physical_index)
                .and_then(Option::take)
                .expect("occupied inline deque slot");
            heap.push_back(entry);
        }
    }

    pub(crate) fn push_front(&mut self, entry: T) {
        if matches!(self, Self::Inline { len, .. } if usize::from(*len) == N) {
            self.spill();
        }
        match self {
            Self::Inline { entries, head, len } => {
                *head = if *head == 0 {
                    u8::try_from(N.saturating_sub(1)).expect("inline capacity fits in a u8")
                } else {
                    head.saturating_sub(1)
                };
                let slot = entries
                    .get_mut(usize::from(*head))
                    .expect("valid inline deque head");
                debug_assert!(slot.is_none());
                *slot = Some(entry);
                *len = len.saturating_add(1);
            }
            Self::Heap(entries) => entries.push_front(entry),
        }
    }

    pub(crate) fn push_back(&mut self, entry: T) {
        if matches!(self, Self::Inline { len, .. } if usize::from(*len) == N) {
            self.spill();
        }
        match self {
            Self::Inline { entries, head, len } => {
                let physical_index = Self::inline_index(usize::from(*head), usize::from(*len));
                let slot = entries
                    .get_mut(physical_index)
                    .expect("valid inline deque tail");
                debug_assert!(slot.is_none());
                *slot = Some(entry);
                *len = len.saturating_add(1);
            }
            Self::Heap(entries) => entries.push_back(entry),
        }
    }

    fn pop_front(&mut self) -> Option<T> {
        match self {
            Self::Inline { entries, head, len } => {
                if *len == 0 {
                    return None;
                }
                let entry = entries.get_mut(usize::from(*head))?.take();
                *head = u8::try_from(Self::inline_index(usize::from(*head), 1))
                    .expect("inline capacity fits in a u8");
                *len = len.saturating_sub(1);
                entry
            }
            Self::Heap(entries) => entries.pop_front(),
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        match self {
            Self::Inline { entries, head, len } => (index < usize::from(*len))
                .then(|| Self::inline_index(usize::from(*head), index))
                .and_then(|physical_index| entries.get(physical_index))
                .and_then(Option::as_ref),
            Self::Heap(entries) => entries.get(index),
        }
    }

    pub(crate) fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        match self {
            Self::Inline { entries, head, len } => (index < usize::from(*len))
                .then(|| Self::inline_index(usize::from(*head), index))
                .and_then(|physical_index| entries.get_mut(physical_index))
                .and_then(Option::as_mut),
            Self::Heap(entries) => entries.get_mut(index),
        }
    }

    fn swap(&mut self, a: usize, b: usize) {
        if a == b {
            return;
        }
        match self {
            Self::Inline { entries, head, len } => {
                assert!(
                    a < usize::from(*len) && b < usize::from(*len),
                    "indices must be in bounds"
                );
                entries.swap(
                    Self::inline_index(usize::from(*head), a),
                    Self::inline_index(usize::from(*head), b),
                );
            }
            Self::Heap(entries) => entries.swap(a, b),
        }
    }

    /// Inserts an element at `index`, shifting elements from the nearer end.
    pub(crate) fn insert(&mut self, index: usize, entry: T) {
        let old_len = self.len();
        assert!(index <= old_len, "index out of bounds");
        if matches!(self, Self::Inline { len, .. } if usize::from(*len) == N) {
            self.spill();
        }
        if let Self::Heap(entries) = self {
            entries.insert(index, entry);
            return;
        }
        if index <= old_len / 2 {
            self.push_front(entry);
            for current_index in 0..index {
                self.swap(current_index, current_index.saturating_add(1));
            }
        } else {
            self.push_back(entry);
            for current_index in (index..old_len).rev() {
                self.swap(current_index, current_index.saturating_add(1));
            }
        }
    }

    pub(crate) fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&T) -> bool,
    {
        if let Self::Heap(entries) = self {
            entries.retain(keep);
            return;
        }
        let original_len = self.len();
        for _ in 0..original_len {
            let retain_front = keep(self.get(0).expect("nonempty deque"));
            let front = self.pop_front().expect("nonempty deque");
            if retain_front {
                self.push_back(front);
            }
        }
    }

    pub(crate) fn binary_search_by<F>(&self, mut compare: F) -> Result<usize, usize>
    where
        F: FnMut(&T) -> Ordering,
    {
        if let Self::Heap(entries) = self {
            return entries.binary_search_by(compare);
        }
        let mut left = 0usize;
        let mut right = self.len();
        while left < right {
            let midpoint = left.saturating_add(right.saturating_sub(left) / 2);
            match compare(self.get(midpoint).expect("midpoint is in bounds")) {
                Ordering::Less => left = midpoint.saturating_add(1),
                Ordering::Greater => right = midpoint,
                Ordering::Equal => return Ok(midpoint),
            }
        }
        Err(left)
    }

    pub(crate) fn iter(&self) -> Iter<'_, T, N> {
        Iter {
            deque: self,
            range: 0..self.len(),
        }
    }

    #[cfg(test)]
    fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }
}

impl<T, const N: usize> Default for SmallVecDeque<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug, const N: usize> fmt::Debug for SmallVecDeque<T, N> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

pub(crate) struct Iter<'a, T, const N: usize> {
    deque: &'a SmallVecDeque<T, N>,
    range: Range<usize>,
}

impl<'a, T, const N: usize> Iterator for Iter<'a, T, N> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().and_then(|index| self.deque.get(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.range.len();
        (len, Some(len))
    }
}

impl<T, const N: usize> DoubleEndedIterator for Iter<'_, T, N> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .next_back()
            .and_then(|index| self.deque.get(index))
    }
}

impl<T, const N: usize> ExactSizeIterator for Iter<'_, T, N> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn values<const N: usize>(deque: &SmallVecDeque<u64, N>) -> Vec<u64> {
        deque.iter().copied().collect()
    }

    #[test]
    fn test_inline_and_spill() {
        let mut deque = SmallVecDeque::<_, 2>::new();
        deque.push_front(2);
        deque.push_front(1);
        assert!(deque.is_inline());
        assert_eq!(values(&deque), [1, 2]);

        deque.push_back(3);
        assert!(!deque.is_inline());
        assert_eq!(values(&deque), [1, 2, 3]);
    }

    #[test]
    fn test_inline_wraparound() {
        let mut deque = SmallVecDeque::<_, 3>::new();
        deque.push_back(1);
        deque.push_back(2);
        assert_eq!(deque.pop_front(), Some(1));
        deque.push_back(3);
        deque.push_front(0);
        assert!(deque.is_inline());
        assert_eq!(values(&deque), [0, 2, 3]);
    }

    #[test]
    fn test_insert() {
        for len in 0..=4 {
            for index in 0..=len {
                let mut deque = SmallVecDeque::<_, 2>::new();
                deque.extend_for_tests(0..len);
                deque.insert(index as usize, 9);
                let mut expected = (0..len).collect::<Vec<_>>();
                expected.insert(index as usize, 9);
                assert_eq!(values(&deque), expected);
            }
        }
    }

    #[test]
    fn test_retain() {
        for len in 0..=5 {
            let mut deque = SmallVecDeque::<_, 2>::new();
            deque.extend_for_tests(0..len);
            let mut visited = Vec::new();
            deque.retain(|value| {
                visited.push(*value);
                value % 2 == 0
            });
            assert_eq!(visited, (0..len).collect::<Vec<_>>());
            assert_eq!(
                values(&deque),
                (0..len).filter(|value| value % 2 == 0).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn test_binary_search_and_mutation() {
        let mut deque = SmallVecDeque::<_, 2>::new();
        deque.extend_for_tests([1, 3, 5, 7]);
        assert_eq!(deque.binary_search_by(|value| value.cmp(&5)), Ok(2));
        assert_eq!(deque.binary_search_by(|value| value.cmp(&4)), Err(2));
        *deque.get_mut(2).unwrap() = 4;
        assert_eq!(values(&deque), [1, 3, 4, 7]);
        assert_eq!(
            deque.iter().rev().copied().collect::<Vec<_>>(),
            [7, 4, 3, 1]
        );
    }

    impl<T, const N: usize> SmallVecDeque<T, N> {
        fn extend_for_tests(&mut self, entries: impl IntoIterator<Item = T>) {
            for entry in entries {
                self.push_back(entry);
            }
        }
    }
}
