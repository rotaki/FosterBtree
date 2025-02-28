use std::ops::Index;

use rand::Rng;

use crate::random::small_thread_rng;

#[derive(Debug, Clone)]
pub struct Permutation {
    pub perm: Vec<usize>,
}

impl Permutation {
    pub fn new(min: usize, max: usize) -> Permutation {
        assert!(min <= max);
        let size = max - min + 1;
        let mut perm: Vec<usize> = Vec::with_capacity(size);

        // Fill perm with values from min to max inclusive
        for i in min..=max {
            perm.push(i);
        }
        assert_eq!(perm.len(), size);

        // Now shuffle perm
        let s = perm.len();
        let mut rng = small_thread_rng();
        for i in 0..(s - 1) {
            let j = rng.random_range(0..=s - i - 1);
            assert!(i + j < s);
            if j != 0 {
                perm.swap(i, i + j);
            }
        }

        Permutation { perm }
    }
}

impl Index<usize> for Permutation {
    type Output = usize;

    fn index(&self, i: usize) -> &Self::Output {
        assert!(i < self.perm.len());
        &self.perm[i]
    }
}

// Implement iterator
impl IntoIterator for Permutation {
    type Item = usize;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.perm.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permutation_elements() {
        for _ in 0..100 {
            let min = 0;
            let max = 9;
            let perm = Permutation::new(min, max);
            let mut elements = perm.perm.clone();
            elements.sort_unstable();
            let expected: Vec<usize> = (min..=max).collect();
            assert_eq!(elements, expected);
        }
    }
}
