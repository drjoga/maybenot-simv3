//! Settings module for creating and configuring network simulation topologies.
//!
//! This module provides pre-configured network topologies with fixed node structures
//! and randomizable link parameters for parallel simulation runs.

pub mod randomize;
pub mod templates;

use crate::topology::{NetworkLinkState, NetworkTopology};

/// A network simulation setting combining topology and link state.
///
/// Settings have fixed node structures (determined by the template) but allow
/// randomization of link parameters (bandwidth, latency) for parallel execution.
///
/// # Example
///
/// ```rust
/// use maybenot_simulatorv3::settings::templates::VpnSetting;
/// use rand::thread_rng;
///
/// // Create a VPN setting with defaults
/// let mut setting = VpnSetting::new().unwrap();
///
/// // Randomize link parameters
/// let mut rng = thread_rng();
/// setting.randomize_link(0, &mut rng).unwrap();
///
/// // Get topology and linkstate for simulation
/// let (topology, mut linkstate) = setting.into_parts();
/// ```
#[derive(Debug)]
pub struct Setting {
    topology: NetworkTopology,
    linkstate: NetworkLinkState,
}

impl Setting {
    /// Create a new Setting from topology and linkstate.
    pub(crate) fn new(topology: NetworkTopology, linkstate: NetworkLinkState) -> Self {
        Self {
            topology,
            linkstate,
        }
    }

    /// Get an immutable reference to the topology.
    pub fn topology(&self) -> &NetworkTopology {
        &self.topology
    }

    /// Get a mutable reference to the link state.
    ///
    /// Use this to directly modify link parameters before running simulations.
    pub fn linkstate_mut(&mut self) -> &mut NetworkLinkState {
        &mut self.linkstate
    }

    /// Consume the setting and return the topology and linkstate for simulation.
    pub fn into_parts(self) -> (NetworkTopology, NetworkLinkState) {
        (self.topology, self.linkstate)
    }

    /// Randomize a specific link's parameters.
    ///
    /// This modifies the link's throughput and propagation delay within ±20% of
    /// their original values (for fixed parameters). Trace-based parameters are
    /// reset for now (future: random offset).
    ///
    /// # Arguments
    ///
    /// * `link_id` - The ID of the link to randomize
    /// * `rng` - Random number generator
    ///
    /// # Errors
    ///
    /// Returns an error if the link_id is invalid.
    pub fn randomize_link<R: rand::Rng>(
        &mut self,
        link_id: usize,
        rng: &mut R,
    ) -> Result<(), String> {
        use randomize::Randomizable;

        let link = self
            .linkstate
            .get_link_mut(link_id)
            .ok_or_else(|| format!("Link {} not found", link_id))?;

        link.randomize(rng);
        Ok(())
    }
}

impl Clone for Setting {
    fn clone(&self) -> Self {
        Self {
            topology: self.topology.new_from_config(),
            linkstate: self.linkstate.clone(),
        }
    }
}
