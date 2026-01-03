//! Randomization utilities for link parameters.

use crate::links::{FixedTputLink, HiTraceTputLink, LinkType, StdTraceTputLink};
use rand::Rng;
use std::time::Duration;

/// Trait for randomizing link parameters.
///
/// Implementors can randomize their throughput and propagation delay parameters
/// for Monte Carlo simulations and parameter sweeps.
pub trait Randomizable {
    /// Randomize this link's parameters using the provided RNG.
    ///
    /// For fixed parameters, this applies ±20% variation.
    /// For trace-based parameters, this applies a random starting offset.
    fn randomize<R: Rng>(&mut self, rng: &mut R);
}

impl Randomizable for LinkType {
    fn randomize<R: Rng>(&mut self, rng: &mut R) {
        match self {
            LinkType::FixedTput(link) => link.randomize(rng),
            LinkType::HiTraceTput(link) => link.randomize(rng),
            LinkType::StdTraceTput(link) => link.randomize(rng),
        }
    }
}

impl Randomizable for FixedTputLink {
    fn randomize<R: Rng>(&mut self, rng: &mut R) {
        // Randomize throughput ±20%
        let delta = (self.tput_bps as f64 * 0.2) as u64;
        let min = self.tput_bps.saturating_sub(delta);
        let max = self.tput_bps.saturating_add(delta);
        self.tput_bps = rng.random_range(min..=max);

        // Randomize propagation ±20% if fixed
        if self.fixed_propagation {
            let delay_us = self.prop_us.as_micros() as u64;
            let delta = (delay_us as f64 * 0.2) as u64;
            let min = delay_us.saturating_sub(delta);
            let max = delay_us.saturating_add(delta);
            self.prop_us = Duration::from_micros(rng.random_range(min..=max));
        }
        // Note: Variable propagation (prop_us_vec) randomization deferred
    }
}

impl Randomizable for HiTraceTputLink {
    fn randomize<R: Rng>(&mut self, rng: &mut R) {
        // Randomize starting offset in trace (now that looping is enabled)
        let trace_len = self.get_trace_len();
        let random_offset = rng.random_range(0..trace_len);
        self.set_random_offset(random_offset);

        // Randomize propagation ±20% if fixed
        if self.fixed_propagation {
            let delay_us = self.prop_us.as_micros() as u64;
            let delta = (delay_us as f64 * 0.2) as u64;
            let min = delay_us.saturating_sub(delta);
            let max = delay_us.saturating_add(delta);
            self.prop_us = Duration::from_micros(rng.random_range(min..=max));
        }
        // Note: Variable propagation (prop_us_vec) randomization deferred
    }
}

impl Randomizable for StdTraceTputLink {
    fn randomize<R: Rng>(&mut self, rng: &mut R) {
        // Randomize starting offset in trace (now that looping is enabled)
        let trace_len = self.get_trace_len();
        let random_offset = rng.random_range(0..trace_len);
        self.set_random_offset(random_offset);

        // Randomize propagation ±20% if fixed
        if self.fixed_propagation {
            let delay_us = self.prop_us.as_micros() as u64;
            let delta = (delay_us as f64 * 0.2) as u64;
            let min = delay_us.saturating_sub(delta);
            let max = delay_us.saturating_add(delta);
            self.prop_us = Duration::from_micros(rng.random_range(min..=max));
        }
        // Note: Variable propagation (prop_us_vec) randomization deferred
    }
}
