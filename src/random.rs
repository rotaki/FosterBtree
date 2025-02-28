use std::cell::RefCell;
use std::ops::Index;

use rand::distr::uniform::SampleUniform;
use rand::distr::weighted::WeightedIndex;
use rand::distr::Alphanumeric;
use rand::rngs::SmallRng;
use rand::{
    distr::{Distribution, Uniform},
    Rng,
};
use rand::{RngCore, SeedableRng};

// Thread-local `SmallRng` state.
thread_local! {
    static THREAD_RNG_KEY: RefCell<SmallRng> = RefCell::new(SmallRng::from_os_rng());
}

/// A handle to the thread-local `SmallRng`—similar to `rand::ThreadRng`.
#[derive(Debug, Clone)]
pub struct SmallThreadRng;

impl RngCore for SmallThreadRng {
    fn next_u32(&mut self) -> u32 {
        THREAD_RNG_KEY.with(|rng_cell| rng_cell.borrow_mut().next_u32())
    }

    fn next_u64(&mut self) -> u64 {
        THREAD_RNG_KEY.with(|rng_cell| rng_cell.borrow_mut().next_u64())
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        THREAD_RNG_KEY.with(|rng_cell| rng_cell.borrow_mut().fill_bytes(dest))
    }
}

pub fn small_thread_rng() -> SmallThreadRng {
    SmallThreadRng
}

pub fn gen_random_pathname(prefix: Option<&str>) -> String {
    let ts_in_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir_name = format!("{}_{}", prefix.unwrap_or("random_path"), ts_in_ns);
    dir_name
}

/// Generates a random alphanumeric string of a specified length.
///
/// # Arguments
///
/// * `length` - The length of the string to generate.
pub fn gen_random_string_with_length(length: usize) -> String {
    small_thread_rng()
        .sample_iter(Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn gen_random_byte_vec_with_length(length: usize) -> Vec<u8> {
    let mut rng = small_thread_rng();
    let range = Uniform::new_inclusive(0, 255).unwrap();
    let mut vec = Vec::with_capacity(length);
    for _ in 0..length {
        vec.push(range.sample(&mut rng) as u8);
    }
    vec
}

/// Generates a random integer within a specified range.
///
/// # Arguments
///
/// * `min` - The minimum value of the integer (inclusive).
/// * `max` - The maximum value of the integer (inclusive).
pub fn gen_random_int<T>(min: T, max: T) -> T
where
    T: SampleUniform,
{
    let mut rng = small_thread_rng();
    rng.sample(Uniform::new_inclusive(min, max).unwrap())
}

pub fn gen_random_string(min: usize, max: usize) -> String {
    let length = gen_random_int(min, max);
    gen_random_string_with_length(length)
}

pub fn gen_random_byte_vec(min: usize, max: usize) -> Vec<u8> {
    let length = gen_random_int(min, max);
    gen_random_byte_vec_with_length(length)
}

pub fn gen_random_permutation<T>(mut vec: Vec<T>) -> Vec<T> {
    let len = vec.len();
    for i in 0..len {
        let j = gen_random_int(i, len - 1);
        vec.swap(i, j);
    }
    vec
}

use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Clone)]
pub struct RandomKVs {
    kvs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl RandomKVs {
    pub fn new(
        unique_keys: bool,
        sorted: bool,
        partitions: usize,
        num_keys: usize,
        key_size: usize,
        val_min_size: usize,
        val_max_size: usize,
    ) -> Vec<Self> {
        let keys = if unique_keys {
            let keys = (0..num_keys).collect::<Vec<usize>>();
            gen_random_permutation(keys)
        } else {
            (0..num_keys)
                .map(|_| gen_random_int(0, num_keys))
                .collect::<Vec<usize>>()
        };

        fn to_bytes(key: usize, key_size: usize) -> Vec<u8> {
            // Pad the key with 0s to make it key_size bytes long.
            let mut key_vec = vec![0u8; key_size];
            let bytes = key.to_be_bytes().to_vec();
            key_vec[..bytes.len()].copy_from_slice(&bytes);
            key_vec
        }

        let mut kvs = Vec::with_capacity(partitions);
        for i in 0..partitions {
            let start = i * num_keys / partitions;
            let end = if i == partitions - 1 {
                num_keys
            } else {
                (i + 1) * num_keys / partitions
            };
            let mut kvs_i = Vec::with_capacity(end - start);
            for key in &keys[start..end] {
                kvs_i.push((
                    to_bytes(*key, key_size),
                    gen_random_byte_vec(val_min_size, val_max_size),
                ));
            }
            if sorted {
                kvs_i.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
            }
            kvs.push(Self { kvs: kvs_i });
        }

        kvs
    }

    pub fn len(&self) -> usize {
        self.kvs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.kvs.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.kvs.iter().map(|(k, v)| (k, v))
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        self.kvs.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
}

impl Index<usize> for RandomKVs {
    type Output = (Vec<u8>, Vec<u8>);

    fn index(&self, index: usize) -> &Self::Output {
        &self.kvs[index]
    }
}

pub struct RandomOp<T> {
    weighted_choices: Vec<(T, f64)>,
    distribution: WeightedIndex<f64>,
}

impl<T> RandomOp<T> {
    pub fn new(weighted_choices: Vec<(T, f64)>) -> Self {
        let weights: Vec<f64> = weighted_choices.iter().map(|&(_, weight)| weight).collect();
        let distribution = WeightedIndex::new(weights).expect("WeightedIndex creation failed");

        RandomOp {
            weighted_choices,
            distribution,
        }
    }

    pub fn get(&self) -> T
    where
        T: Clone,
    {
        let mut rng = small_thread_rng();
        let index = self.distribution.sample(&mut rng);
        self.weighted_choices[index].0.clone()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RandomVals {
    vals: Vec<Vec<u8>>,
}

impl RandomVals {
    pub fn new(num_vals: usize, partitions: usize, min_size: usize, max_size: usize) -> Vec<Self> {
        let mut vals = Vec::with_capacity(partitions);

        // Total number is num_vals
        // Each partition has num_vals/partitions values except the last one
        for i in 0..partitions {
            let start = i * num_vals / partitions;
            let end = if i == partitions - 1 {
                num_vals
            } else {
                (i + 1) * num_vals / partitions
            };
            let mut vals_i = Vec::with_capacity(end - start);
            for _ in start..end {
                vals_i.push(gen_random_byte_vec(min_size, max_size));
            }
            vals.push(Self { vals: vals_i });
        }

        vals
    }

    pub fn len(&self) -> usize {
        self.vals.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vals.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.vals.iter()
    }

    pub fn get(&self, index: usize) -> Option<&Vec<u8>> {
        self.vals.get(index)
    }
}

impl Index<usize> for RandomVals {
    type Output = Vec<u8>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.vals[index]
    }
}

pub struct FastZipf {
    rng: SmallRng,
    nr: usize,
    alpha: f64,
    zetan: f64,
    eta: f64,
    threshold: f64,
}

impl FastZipf {
    /// Constructs a new `FastZipf` distribution with parameter `theta` over `nr` items.
    ///
    /// # Panics
    /// Panics if `theta < 0.0` or `theta >= 1.0` or if `nr < 1`.
    pub fn new(rng: SmallRng, theta: f64, nr: usize) -> Self {
        assert!(nr >= 1, "nr must be at least 1");
        assert!(
            (0.0..1.0).contains(&theta),
            "theta must be in [0,1) for FastZipf"
        );

        // Precompute zeta(nr, theta)
        let zetan = Self::zeta(nr, theta);

        // compute alpha, eta, threshold
        let alpha = 1.0 / (1.0 - theta);
        let eta = {
            let numerator = 1.0 - (2.0 / nr as f64).powf(1.0 - theta);
            let denominator = 1.0 - Self::zeta(2, theta) / zetan;
            numerator / denominator
        };
        let threshold = 1.0 + 0.5f64.powf(theta);

        FastZipf {
            rng,
            nr,
            alpha,
            zetan,
            eta,
            threshold,
        }
    }

    /// Constructs a `FastZipf` if you already have a precomputed `zetan` (zeta(nr, theta)).
    #[allow(dead_code)]
    pub fn with_zeta(rng: SmallRng, theta: f64, nr: usize, zetan: f64) -> Self {
        assert!(nr >= 1, "nr must be at least 1");
        assert!(
            (0.0..1.0).contains(&theta),
            "theta must be in [0,1) for FastZipf"
        );

        let alpha = 1.0 / (1.0 - theta);
        let eta = {
            let numerator = 1.0 - (2.0 / nr as f64).powf(1.0 - theta);
            let denominator = 1.0 - Self::zeta(2, theta) / zetan;
            numerator / denominator
        };
        let threshold = 1.0 + 0.5f64.powf(theta);

        FastZipf {
            rng,
            nr,
            alpha,
            zetan,
            eta,
            threshold,
        }
    }

    /// Samples a value in `[0, nr)`.
    pub fn sample(&mut self) -> usize {
        // Generate u in [0,1).
        let u = self.rand_f64();
        let uz = u * self.zetan;

        if uz < 1.0 {
            return 0;
        }
        if uz < self.threshold {
            return 1;
        }
        // main formula
        let val = (self.nr as f64) * ((self.eta * u) - self.eta + 1.0).powf(self.alpha);
        val as usize
    }

    /// Returns a raw 64-bit random value.
    pub fn rand_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Returns a random f64 in `[0, 1)`.
    fn rand_f64(&mut self) -> f64 {
        (self.rng.next_u64() as f64) / (u64::MAX as f64)
    }

    /// Computes the zeta function for `nr` terms with exponent `theta`.
    ///
    /// \[
    ///   \zeta(nr, \theta) = \sum_{i=1}^{nr} \frac{1}{i^\theta}
    /// \]
    #[inline]
    pub fn zeta(nr: usize, theta: f64) -> f64 {
        let mut sum = 0.0;
        for i in 1..=nr {
            sum += (1.0 / (i as f64)).powf(theta);
        }
        sum
    }
}
