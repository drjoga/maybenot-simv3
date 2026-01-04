//! Settings module for creating pre-configured network simulation topologies.
//!
//! This module provides templates for common network topologies used in traffic
//! analysis defense simulations. Each template returns a (NetworkTopology, NetworkLinkState)
//! tuple that can be used directly with the simulator.
//!
//! # Example
//!
//! ```rust
//! use maybenot_simulatorv3::settings::Setting;
//! use rand::thread_rng;
//!
//! // Create topology and linkstate from template
//! let (topology, linkstate) = Setting::Vpn.create();
//!
//! // Clone and randomize for parallel simulation runs (±20% variation)
//! let mut rng = thread_rng();
//! let randomized_linkstate = linkstate.clone_randomized(&mut rng, 0.2);
//! ```

use crate::links::LinkType;
use crate::topology::{NetworkLinkState, NetworkTopology};
use std::time::Duration;

// Embed TOML files at compile time from templates subfolder
const VPN_TOML: &str = include_str!("templates/vpn.toml");
const MULTIHOP_GUARD_TOML: &str = include_str!("templates/multihop_guard.toml");
const MULTIHOP_EXIT_TOML: &str = include_str!("templates/multihop_exit.toml");

/// Available network topology settings for traffic analysis defense simulations.
///
/// Each setting creates a different network topology with varying numbers of
/// nodes and links. The topologies differ in where Maybenot defenses run.
///
/// # Example
///
/// ```rust
/// use maybenot_simulatorv3::settings::Setting;
/// use rand::thread_rng;
///
/// // Create topology and linkstate
/// let (topology, linkstate) = Setting::Vpn.create();
///
/// // Clone and randomize for parallel runs (±20% variation)
/// let mut rng = thread_rng();
/// let randomized_linkstate = linkstate.clone_randomized(&mut rng, 0.2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Setting {
    /// VPN topology: Client (Maybenot) ↔ Relay (Maybenot) ↔ Endpoint
    ///
    /// This creates a simple VPN topology with 3 nodes and 4 bidirectional links.
    /// The client and relay both run Maybenot defenses.
    ///
    /// # Topology
    ///
    /// - Node 0: ClientMaybenot
    /// - Node 1: RelayMaybenot
    /// - Node 2: EndpointBasic
    ///
    /// # Links (default parameters)
    ///
    /// - Links 0-1: Client ↔ Relay (100 Mbps, 10ms propagation)
    /// - Links 2-3: Relay ↔ Endpoint (1 Gbps, 5ms propagation)
    Vpn,

    /// VPN topology with custom client-relay link parameters.
    ///
    /// Same as `Vpn` but with user-specified throughput and round-trip time
    /// for the client ↔ relay link (links 0 and 1).
    VpnCustom {
        /// Throughput in megabits per second for client ↔ relay link
        mbps: u64,
        /// Round-trip time for client ↔ relay link (propagation = rtt/2)
        rtt: Duration,
    },

    /// Multi-hop topology with Maybenot on Guard node.
    ///
    /// This creates a 2-hop topology where the first hop (guard) runs Maybenot
    /// defenses. The topology has 4 nodes and 6 bidirectional links.
    ///
    /// # Topology
    ///
    /// - Node 0: ClientMaybenot
    /// - Node 1: RelayMaybenot (Guard/first hop)
    /// - Node 2: RouterBasic (Exit/second hop)
    /// - Node 3: EndpointBasic
    ///
    /// # Links (default parameters)
    ///
    /// - Links 0-1: Client ↔ Guard (100 Mbps, 10ms propagation)
    /// - Links 2-3: Guard ↔ Exit (1 Gbps, 5ms propagation)
    /// - Links 4-5: Exit ↔ Endpoint (1 Gbps, 5ms propagation)
    MultihopGuard,

    /// Multi-hop topology with Maybenot on Exit node.
    ///
    /// This creates a 2-hop topology where the second hop (exit) runs Maybenot
    /// defenses. The topology has 4 nodes and 6 bidirectional links.
    ///
    /// # Topology
    ///
    /// - Node 0: ClientMaybenot
    /// - Node 1: RouterBasic (Guard/first hop)
    /// - Node 2: RelayMaybenot (Exit/second hop)
    /// - Node 3: EndpointBasic
    ///
    /// # Links (default parameters)
    ///
    /// - Links 0-1: Client ↔ Guard (100 Mbps, 10ms propagation)
    /// - Links 2-3: Guard ↔ Exit (1 Gbps, 5ms propagation)
    /// - Links 4-5: Exit ↔ Endpoint (1 Gbps, 5ms propagation)
    MultihopExit,
}

impl Setting {
    /// Create a network topology and linkstate with default parameters.
    ///
    /// Returns a tuple of (NetworkTopology, NetworkLinkState) that can be used
    /// directly with the simulator. The linkstate can be cloned and randomized
    /// for parallel simulation runs.
    ///
    /// # Example
    ///
    /// ```rust
    /// use maybenot_simulatorv3::settings::Setting;
    ///
    /// let (topology, linkstate) = Setting::Vpn.create();
    /// assert_eq!(linkstate.link_count(), 4);
    ///
    /// let (topology, linkstate) = Setting::MultihopGuard.create();
    /// assert_eq!(linkstate.link_count(), 6);
    /// ```
    pub fn create(&self) -> (NetworkTopology, NetworkLinkState) {
        match self {
            Setting::Vpn | Setting::VpnCustom { .. } => {
                let (topology, mut linkstate) = crate::load_topology_from_str(VPN_TOML)
                    .expect("embedded TOML template is valid");

                // Apply custom parameters if VpnCustom
                if let Setting::VpnCustom { mbps, rtt } = self {
                    // Convert mbps to bps, rtt to one-way propagation delay
                    let tput_bps = mbps * 1_000_000;
                    let prop = *rtt / 2;

                    // Modify links 0 (client upstream) and 1 (client downstream)
                    for link_id in [0, 1] {
                        if let Some(LinkType::FixedTput(link)) = linkstate.get_link_mut(link_id) {
                            link.tput_bps = tput_bps;
                            link.prop_us = prop;
                        }
                    }
                }
                (topology, linkstate)
            }
            Setting::MultihopGuard => crate::load_topology_from_str(MULTIHOP_GUARD_TOML)
                .expect("embedded TOML template is valid"),
            Setting::MultihopExit => crate::load_topology_from_str(MULTIHOP_EXIT_TOML)
                .expect("embedded TOML template is valid"),
        }
    }
}
