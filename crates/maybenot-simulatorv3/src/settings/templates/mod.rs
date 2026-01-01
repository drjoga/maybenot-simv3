//! Pre-configured network topology templates.
//!
//! This module provides convenient templates for common network topologies used
//! in traffic analysis defense simulations. Each template provides a fixed node
//! structure with default link parameters that can be randomized for parallel
//! simulation runs.

use crate::settings::Setting;

// Embed TOML files at compile time
const VPN_TOML: &str = include_str!("vpn.toml");
const TWOHOP_VPN_GUARD_TOML: &str = include_str!("twohop_vpn_guard.toml");
const TWOHOP_VPN_EXIT_TOML: &str = include_str!("twohop_vpn_exit.toml");

/// VPN topology template: Client (Maybenot) ↔ VPN Server (Maybenot) ↔ Endpoint
///
/// This creates a simple VPN topology with 3 nodes and 4 bidirectional links.
/// The client and VPN server both run Maybenot defenses.
///
/// # Topology
///
/// - Node 0: ClientMaybenot
/// - Node 1: RelayMaybenot (VPN server)
/// - Node 2: EndpointBasic
///
/// # Links (default parameters)
///
/// - Links 0-1: Client ↔ VPN (100 Mbps, 10ms propagation)
/// - Links 2-3: VPN ↔ Endpoint (1 Gbps, 5ms propagation)
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
/// // Randomize client link parameters
/// let mut rng = thread_rng();
/// setting.randomize_link(0, &mut rng).unwrap();
///
/// // Get topology and linkstate for simulation
/// let (topology, mut linkstate) = setting.into_parts();
/// ```
pub struct VpnSetting;

impl VpnSetting {
    /// Create a new VPN setting with default parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded TOML template cannot be parsed.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Result<Setting, String> {
        let (topology, linkstate) = crate::load_topology_from_str(VPN_TOML)?;
        Ok(Setting::new(topology, linkstate))
    }
}

/// Link IDs for the VPN topology.
///
/// Use these constants to identify specific links when randomizing parameters.
pub mod vpn_links {
    /// Client → VPN Server (upstream)
    pub const CLIENT_UPSTREAM: usize = 0;
    /// VPN Server → Client (downstream)
    pub const CLIENT_DOWNSTREAM: usize = 1;
    /// VPN Server → Endpoint (upstream)
    pub const VPN_UPSTREAM: usize = 2;
    /// Endpoint → VPN Server (downstream)
    pub const VPN_DOWNSTREAM: usize = 3;
}

/// 2-hop VPN topology with Maybenot on Guard node.
///
/// This creates a 2-hop VPN topology where the first hop (guard) runs Maybenot
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
///
/// # Example
///
/// ```rust
/// use maybenot_simulatorv3::settings::templates::TwoHopVpnGuardSetting;
/// use rand::thread_rng;
///
/// // Create a 2-hop VPN setting with Maybenot on guard
/// let mut setting = TwoHopVpnGuardSetting::new().unwrap();
///
/// // Randomize client link parameters
/// let mut rng = thread_rng();
/// setting.randomize_link(0, &mut rng).unwrap();
///
/// // Get topology and linkstate for simulation
/// let (topology, mut linkstate) = setting.into_parts();
/// ```
pub struct TwoHopVpnGuardSetting;

impl TwoHopVpnGuardSetting {
    /// Create a new 2-hop VPN setting with Maybenot on the guard node.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded TOML template cannot be parsed.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Result<Setting, String> {
        let (topology, linkstate) = crate::load_topology_from_str(TWOHOP_VPN_GUARD_TOML)?;
        Ok(Setting::new(topology, linkstate))
    }
}

/// 2-hop VPN topology with Maybenot on Exit node.
///
/// This creates a 2-hop VPN topology where the second hop (exit) runs Maybenot
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
///
/// # Example
///
/// ```rust
/// use maybenot_simulatorv3::settings::templates::TwoHopVpnExitSetting;
/// use rand::thread_rng;
///
/// // Create a 2-hop VPN setting with Maybenot on exit
/// let mut setting = TwoHopVpnExitSetting::new().unwrap();
///
/// // Randomize client link parameters
/// let mut rng = thread_rng();
/// setting.randomize_link(0, &mut rng).unwrap();
///
/// // Get topology and linkstate for simulation
/// let (topology, mut linkstate) = setting.into_parts();
/// ```
pub struct TwoHopVpnExitSetting;

impl TwoHopVpnExitSetting {
    /// Create a new 2-hop VPN setting with Maybenot on the exit node.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded TOML template cannot be parsed.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Result<Setting, String> {
        let (topology, linkstate) = crate::load_topology_from_str(TWOHOP_VPN_EXIT_TOML)?;
        Ok(Setting::new(topology, linkstate))
    }
}

/// Link IDs for the 2-hop VPN topologies (both guard and exit variants).
///
/// Use these constants to identify specific links when randomizing parameters.
pub mod twohop_vpn_links {
    /// Client → First hop (upstream)
    pub const CLIENT_UPSTREAM: usize = 0;
    /// First hop → Client (downstream)
    pub const CLIENT_DOWNSTREAM: usize = 1;
    /// First hop → Second hop (upstream)
    pub const GUARD_UPSTREAM: usize = 2;
    /// Second hop → First hop (downstream)
    pub const GUARD_DOWNSTREAM: usize = 3;
    /// Second hop → Endpoint (upstream)
    pub const EXIT_UPSTREAM: usize = 4;
    /// Endpoint → Second hop (downstream)
    pub const EXIT_DOWNSTREAM: usize = 5;
}
