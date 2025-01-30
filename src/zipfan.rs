use rand::RngCore;

use crate::random::SmallThreadRng;

pub struct FastZipf {
    rng: SmallThreadRng,
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
    pub fn new(rng: SmallThreadRng, theta: f64, nr: usize) -> Self {
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
    pub fn with_zeta(rng: SmallThreadRng, theta: f64, nr: usize, zetan: f64) -> Self {
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
