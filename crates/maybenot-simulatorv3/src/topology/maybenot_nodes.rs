use crate::integration::Integration;
use crate::maybenot_helpers::{
    maybenot_do_internal_timer, maybenot_do_scheduled_action, maybenot_trigger_update,
};
use crate::topology::nodes::check_dependent_packets;
use crate::topology::{NetworkLinkState, NetworkTopology};
use crate::{SimEvent, SimInfo, SimQueue};
use log::debug;
use maybenot::{Framework, Machine, TriggerAction, TriggerEvent};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rand::{RngCore, rngs::ThreadRng};
use rand_xoshiro::Xoshiro256StarStar;
use rand_xoshiro::rand_core::SeedableRng;

// Enum to encapsulate different RngCore sources: in the Maybenot Framework, the
// RngCore trait is not ?Sized (unnecessary overhead for the framework), so we
// have to work around this by using an enum to support selecting rng source as
// a simulation option.
#[derive(Debug, Clone)]
pub enum RngSource {
    Thread(ThreadRng),
    Xoshiro(Xoshiro256StarStar),
}

impl RngCore for RngSource {
    fn next_u32(&mut self) -> u32 {
        match self {
            RngSource::Thread(rng) => rng.next_u32(),
            RngSource::Xoshiro(rng) => rng.next_u32(),
        }
    }

    fn next_u64(&mut self) -> u64 {
        match self {
            RngSource::Thread(rng) => rng.next_u64(),
            RngSource::Xoshiro(rng) => rng.next_u64(),
        }
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        match self {
            RngSource::Thread(rng) => rng.fill_bytes(dest),
            RngSource::Xoshiro(rng) => rng.fill_bytes(dest),
        }
    }
}

/// ScheduledAction represents an action that is scheduled to be executed at a
/// certain time.
#[derive(PartialEq, Clone, Debug)]
pub struct ScheduledAction {
    pub action: TriggerAction,
    pub time: Instant,
}

/// The state of the client, or relay in the simulator.
#[derive(Debug, Clone)]
pub struct MaybenotState<M, R> {
    /// an instance of the Maybenot framework
    pub framework: Framework<M, R>,
    /// scheduled action timers
    pub scheduled_action: Vec<Option<ScheduledAction>>,
    /// scheduled internal timers
    pub scheduled_internal_timer: Vec<Option<Instant>>,
    /// blocking until time, active is set
    pub blocking_until: Option<Instant>,
    /// whether the active blocking bypassable or not
    pub blocking_bypassable: bool,
    /// whether to drain blocked packets by time or first all normal then
    /// padding
    pub drain_blocked_by_time: bool,
    /// integration aspects for this state
    pub integration: Option<Integration>,
}

impl<M> MaybenotState<M, RngSource>
where
    M: AsRef<[Machine]>,
{
    pub(crate) fn new(
        machines: M,
        current_time: Instant,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        drain_blocked_by_time: bool,
        integration: Option<Integration>,
        insecure_rng_seed: Option<u64>,
    ) -> Self {
        let rng = match insecure_rng_seed {
            // deterministic, insecure RNG
            Some(seed) => RngSource::Xoshiro(Xoshiro256StarStar::seed_from_u64(seed)),
            // secure RNG, default
            None => RngSource::Thread(rand::rng()),
        };

        let num_machines = machines.as_ref().len();

        Self {
            framework: Framework::new(
                machines,
                max_padding_frac,
                max_blocking_frac,
                current_time,
                rng,
            )
            .unwrap(),
            scheduled_action: vec![None; num_machines],
            scheduled_internal_timer: vec![None; num_machines],
            blocking_until: None,
            blocking_bypassable: false,
            drain_blocked_by_time,
            integration,
        }
    }

    pub(crate) fn reporting_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(Integration::reporting_delay)
            .unwrap_or(Duration::from_micros(0))
    }

    pub(crate) fn action_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(Integration::action_delay)
            .unwrap_or(Duration::from_micros(0))
    }

    pub(crate) fn trigger_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(Integration::trigger_delay)
            .unwrap_or(Duration::from_micros(0))
    }
}

// Implements the core traffic shaping logic for Maybenot defenses.
//
// This function determines whether a packet (normal or padding) should be:
// 1. Sent immediately (no blocking active)
// 2. Queued for later (blocked, non-bypassable)
// 3. Bypassed through blocking (blocked but bypassable)
// 4. Replaced with queued normal traffic (padding with replace=true)
pub(crate) fn maybenot_handle_tunnel_sent_creation<T: MaybenotNode>(
    node: &T,
    s_event: SimEvent,
    sq: &mut SimQueue,
) {
    let sim_state = node.get_sim_state().borrow();
    let blocking_bypassable = sim_state.blocking_bypassable;
    let blocking_until = sim_state.blocking_until;
    drop(sim_state); // Release borrow before queuing

    // Check if we're currently blocking
    if let Some(blocking_until) = blocking_until {
        if s_event.time < blocking_until {
            // We're in blocking period

            if blocking_bypassable && s_event.bypass {
                // The blocking is bypassable

                // replace flag is set: if we have a normal packet queued up /
                // blocked, we can replace the padding with that FIXME: here be
                // bugs related to integration delays
                if s_event.contains_padding {
                    if s_event.replace {
                        // Check if we have a normal packet queued up
                        let mut normal_queue = node.get_queue_normal().borrow_mut();

                        if let Some(mut dequeued_normal_event) = normal_queue.pop_front() {
                            dequeued_normal_event.time = s_event.time;
                            dequeued_normal_event.bypass = true;
                            debug!(
                                "Replacing bypass padding with normal event: {:?}",
                                dequeued_normal_event
                            );
                            sq.push(dequeued_normal_event);
                            return;
                        } else {
                            debug!("No normal to replace with, sending bypass padding");
                        }
                    } else {
                        debug!("Sending bypass padding, Replace not set");
                    }
                } else {
                    debug!("Sending bypass Normal packet");
                }
            // Below here we could not bypass
            } else if s_event.contains_padding {
                if s_event.replace && !node.get_queue_normal().borrow().is_empty() {
                    // If padding_replace and there is a blocked normal packet
                    // the padding is replaced, i.e. not enqueued
                    debug!("Padding replaced by blocked normal packet, nothing enqueued");
                    return;
                } else {
                    node.get_queue_padding().borrow_mut().push_back(s_event);
                    debug!("Blocked Padding enqueued");
                    return;
                }
            } else {
                node.get_queue_normal().borrow_mut().push_back(s_event);
                debug!("Blocking Normal enqued");
                return;
            }
        }
    }
    // Not blocking or past blocking time or bypass fallthrough - add to
    // simulation queue immediately
    debug!("TunnelSent immediately");
    sq.push(s_event);
}

// Releases all queued events when a blocking period expires.
//
// Two drainage strategies are supported:
// 1. Time-ordered: Events drain in chronological order by original timestamp
// 2. Type-ordered: All normal packets first, then all padding packets
pub(crate) fn maybenot_release_blocked_events<T: MaybenotNode>(
    node: &T,
    sq: &mut SimQueue,
    current_time: Instant,
    drain_blocked_by_time: bool,
) {
    // Release all events from both queues
    let mut padding_events = node.get_queue_padding().borrow_mut();
    let mut normal_events = node.get_queue_normal().borrow_mut();

    debug!(
        "Releasing {} padding events and {} normal events",
        padding_events.len(),
        normal_events.len()
    );

    if drain_blocked_by_time {
        // Time-wise draining: release packets in chronological order based on
        // their original timestamps
        loop {
            // Check the earliest event from each queue
            let earliest_padding = padding_events.front().map(|e| e.time);
            let earliest_normal = normal_events.front().map(|e| e.time);

            // Determine which queue has the earliest event
            let drain_padding = match (earliest_padding, earliest_normal) {
                (Some(p_time), Some(n_time)) => p_time <= n_time,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break, // Both queues are empty
            };

            // Drain the earliest event and add it to simulation queue
            if drain_padding {
                if let Some(mut event) = padding_events.pop_front() {
                    debug!(
                        "Releasing padding event (time-wise): {:?} originally at {:?}, now at {:?}",
                        event.event, event.time, current_time
                    );
                    event.time = current_time;
                    sq.push(event);
                }
            } else if let Some(mut event) = normal_events.pop_front() {
                debug!(
                    "Releasing normal event (time-wise): {:?} originally at {:?}, now at {:?}",
                    event.event, event.time, current_time
                );
                event.time = current_time;
                sq.push(event);
            }
        }
    } else {
        // Move all normal queue events to simulation queue with updated time
        for mut event in normal_events.drain(..) {
            debug!(
                "Releasing normal event: {:?} originally at {:?}, now at {:?}",
                event.event, event.time, current_time
            );
            event.time = current_time;
            sq.push(event);
        }

        // Move all padding queue events to simulation queue with updated time
        for mut event in padding_events.drain(..) {
            debug!(
                "Releasing padding event: {:?} originally at {:?}, now at {:?}",
                event.event, event.time, current_time
            );
            event.time = current_time;
            sq.push(event);
        }
    }
}

/// Trait for Maybenot nodes to enable generic implementations.
///
/// # Internal API
///
/// This trait is exposed for testing. The `node_id()` method is the primary
/// public interface for identifying nodes in simulation output.
#[allow(private_interfaces)]
pub trait MaybenotNode {
    fn get_sim_state(&self) -> &RefCell<MaybenotState<Vec<Machine>, RngSource>>;
    /// Returns the node ID for this Maybenot node.
    fn node_id(&self) -> usize;
    fn get_action_link_id(&self) -> usize; // Link used for actions (coreside for client, edgeside for relay)
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimEvent>>;
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimEvent>>;

    // Methods needed for simulation
    fn trigger_update(
        &self,
        s_event: &SimEvent,
        current_time: &Instant,
        sq: &mut SimQueue,
        topology: &NetworkTopology,
    );
    fn do_internal_timer(&self, target: Instant) -> Option<SimEvent>;
    fn do_scheduled_action(&self, target: Instant) -> Option<SimEvent>;
}

/// A Maybenot-enabled client node that originates traffic with defense
/// mechanisms.
///
/// # Network Topology Directionality
///
/// In the simulator, nodes have directional links defined by their position in
/// the network:
/// - **coreside**: Links toward the endpoint (core of the network, away from
///   edge)
/// - **edgeside**: Links toward the client (edge of the network, back toward
///   origin)
///
/// Client nodes originate traffic and send defense actions (padding, blocking)
/// toward the endpoint, so they only have `coreside_out`.
///
/// # Defense Action Link
///
/// For ClientMaybenot, all Maybenot defense actions (padding, blocking) are
/// sent on the **coreside_out** link. This is returned by
/// `get_action_link_id()`.
#[derive(Debug, Clone)]
pub struct ClientMaybenot {
    pub id: usize,
    /// Link ID for outgoing traffic and defense actions toward the endpoint (forward direction)
    pub coreside_out: usize,
    pub sim_state: RefCell<MaybenotState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimEvent>>,
    pub queue_normal: RefCell<VecDeque<SimEvent>>,
}

impl MaybenotNode for ClientMaybenot {
    fn get_sim_state(&self) -> &RefCell<MaybenotState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }

    fn node_id(&self) -> usize {
        self.id
    }

    fn get_action_link_id(&self) -> usize {
        self.coreside_out
    }

    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimEvent>> {
        &self.queue_padding
    }

    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimEvent>> {
        &self.queue_normal
    }

    fn trigger_update(
        &self,
        s_event: &SimEvent,
        current_time: &Instant,
        sq: &mut SimQueue,
        topology: &NetworkTopology,
    ) {
        maybenot_trigger_update(self, s_event, current_time, sq, topology)
    }

    fn do_internal_timer(&self, target: Instant) -> Option<SimEvent> {
        maybenot_do_internal_timer(self, target)
    }

    fn do_scheduled_action(&self, target: Instant) -> Option<SimEvent> {
        maybenot_do_scheduled_action(self, target)
    }
}

#[allow(clippy::too_many_arguments)]
impl ClientMaybenot {
    pub fn new(
        id: usize,
        coreside_out: usize,
        machines: Vec<Machine>,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        drain_blocked_by_time: bool,
        integration: Option<Integration>,
        insecure_rng_seed: Option<u64>,
    ) -> Self {
        let sim_state = RefCell::new(MaybenotState::new(
            machines,
            Instant::now(),
            max_padding_frac,
            max_blocking_frac,
            drain_blocked_by_time,
            integration,
            insecure_rng_seed,
        ));

        Self {
            id,
            coreside_out,
            sim_state,
            queue_padding: RefCell::new(VecDeque::new()),
            queue_normal: RefCell::new(VecDeque::new()),
        }
    }

    pub fn handle_event(
        &self,
        s_event: &SimEvent,
        topology: &NetworkTopology,
        linkstate: &mut NetworkLinkState,
        si: &SimInfo,
        sq: &mut SimQueue,
    ) {
        match &s_event.event {
            TriggerEvent::NormalSent => {
                let forward_s_event = SimEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time,
                    packet_id: s_event.packet_id,
                    node_id: s_event.node_id,
                    link_id: s_event.link_id,
                    contains_padding: false,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                // Use blocking-aware logic to decide whether to queue
                // immediately or block
                maybenot_handle_tunnel_sent_creation(self, forward_s_event, sq);
            }

            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time,
                    packet_id: s_event.packet_id,
                    node_id: s_event.node_id,
                    link_id: s_event.link_id,
                    contains_padding: true,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                // Use blocking-aware logic to decide whether to queue
                // immediately or block
                maybenot_handle_tunnel_sent_creation(self, forward_s_event, sq);
            }

            TriggerEvent::TunnelSent => {
                crate::topology::nodes::make_network_receive_from_sent(
                    s_event, topology, linkstate, si, sq,
                );
            }

            TriggerEvent::TunnelRecv => {
                let new_t_event = match &s_event.contains_padding {
                    true => TriggerEvent::PaddingRecv,
                    false => TriggerEvent::NormalRecv,
                };
                let forward_s_event = SimEvent {
                    event: new_t_event,
                    time: s_event.time,
                    packet_id: s_event.packet_id,
                    node_id: s_event.node_id,
                    link_id: s_event.link_id,
                    contains_padding: s_event.contains_padding,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_s_event);
            }

            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_id].get_coreside_out_id();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                crate::topology::nodes::check_dependent_packets(s_event, si, sq, outgoing_link, 0);
            }

            TriggerEvent::BlockingEnd => {
                let mut state = self.sim_state.borrow_mut();
                // Release any queued events with current time
                maybenot_release_blocked_events(
                    self,
                    sq,
                    s_event.time,
                    state.drain_blocked_by_time,
                );

                // Clear blocking state
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }
}

/// A Maybenot-enabled relay node that forwards traffic bidirectionally with
/// defense mechanisms.
///
/// # Network Topology Directionality
///
/// Relay nodes sit in the middle of the network path and forward traffic
/// bidirectionally:
/// - **coreside_out**: Forwards traffic toward the endpoint (away from
///   client)
/// - **edgeside_in**: Receives traffic from coreside (from endpoint
///   direction)
/// - **edgeside_out**: Forwards traffic back toward the client (return path)
///
/// Traffic flow through a relay:
/// ```text
/// Client --> Relay --[coreside_out]--> Endpoint
///            Relay <-[edgeside_in]---- Endpoint
/// Client <-[edgeside_out]-- Relay <--- Endpoint
/// ```
///
/// # Defense Action Link
///
/// For RelayMaybenot, all Maybenot defense actions (padding, blocking) are sent
/// on the **edgeside_out** link (back toward the client). This is returned by
/// `get_action_link_id()`.
///
/// This is different from ClientMaybenot, which uses coreside_out for actions.
#[derive(Debug, Clone)]
pub struct RelayMaybenot {
    pub id: usize,
    /// Link ID for forwarding traffic toward the endpoint (forward
    /// direction)
    pub coreside_out: usize,
    /// Link ID for receiving traffic from the coreside (from endpoint
    /// direction)
    pub edgeside_in: usize,
    /// Link ID for forwarding traffic and defense actions back toward the
    /// client (return direction)
    pub edgeside_out: usize,
    pub sim_state: RefCell<MaybenotState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimEvent>>,
    pub queue_normal: RefCell<VecDeque<SimEvent>>,
}

impl MaybenotNode for RelayMaybenot {
    fn get_sim_state(&self) -> &RefCell<MaybenotState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }

    fn node_id(&self) -> usize {
        self.id
    }

    fn get_action_link_id(&self) -> usize {
        self.edgeside_out
    }

    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimEvent>> {
        &self.queue_padding
    }

    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimEvent>> {
        &self.queue_normal
    }

    fn trigger_update(
        &self,
        s_event: &SimEvent,
        current_time: &Instant,
        sq: &mut SimQueue,
        topology: &NetworkTopology,
    ) {
        maybenot_trigger_update(self, s_event, current_time, sq, topology)
    }

    fn do_internal_timer(&self, target: Instant) -> Option<SimEvent> {
        maybenot_do_internal_timer(self, target)
    }

    fn do_scheduled_action(&self, target: Instant) -> Option<SimEvent> {
        maybenot_do_scheduled_action(self, target)
    }
}

impl RelayMaybenot {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        coreside_out: usize,
        edgeside_in: usize,
        edgeside_out: usize,
        machines: Vec<Machine>,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        drain_blocked_by_time: bool,
        integration: Option<Integration>,
        insecure_rng_seed: Option<u64>,
    ) -> Self {
        let sim_state = RefCell::new(MaybenotState::new(
            machines,
            Instant::now(),
            max_padding_frac,
            max_blocking_frac,
            drain_blocked_by_time,
            integration,
            insecure_rng_seed,
        ));

        Self {
            id,
            coreside_out,
            edgeside_in,
            edgeside_out,
            sim_state,
            queue_padding: RefCell::new(VecDeque::new()),
            queue_normal: RefCell::new(VecDeque::new()),
        }
    }

    pub fn handle_event(
        &self,
        s_event: &SimEvent,
        topology: &NetworkTopology,
        linkstate: &mut NetworkLinkState,
        si: &SimInfo,
        sq: &mut SimQueue,
    ) {
        match &s_event.event {
            TriggerEvent::TunnelRecv => {
                let new_event = match &s_event.contains_padding {
                    true => TriggerEvent::PaddingRecv,
                    false => TriggerEvent::NormalRecv,
                };
                let forward_event = SimEvent {
                    event: new_event,
                    time: s_event.time,
                    packet_id: s_event.packet_id,
                    node_id: s_event.node_id,
                    link_id: s_event.link_id,
                    contains_padding: false,
                    bypass: false,
                    replace: false,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_event);
            }

            TriggerEvent::NormalRecv => {
                let outlink = topology
                    .get_outlink(s_event.node_id, s_event.link_id)
                    .unwrap();
                if outlink == self.coreside_out {
                    crate::topology::nodes::forward_network_receive_from_receive(
                        s_event, topology, linkstate, si, sq,
                    );
                } else if outlink == self.edgeside_out {
                    let new_s_event = SimEvent {
                        event: TriggerEvent::NormalSent,
                        time: s_event.time,
                        packet_id: s_event.packet_id,
                        node_id: s_event.node_id,
                        link_id: outlink,
                        contains_padding: false,
                        bypass: false,
                        replace: false,
                        q_sequence_nr: 0, // Will be overwritten by push()
                        #[cfg(debug_assertions)]
                        debug_note: None,
                    };
                    sq.push(new_s_event);
                } else {
                    panic!(
                        "RelayMaybenot received NormalRecv on unexpected link index: {}",
                        s_event.link_id
                    );
                }
            }

            TriggerEvent::NormalSent => {
                if s_event.link_id == self.coreside_out {
                    crate::topology::nodes::make_network_receive_from_sent(
                        s_event, topology, linkstate, si, sq,
                    );
                } else if s_event.link_id == self.edgeside_out {
                    let forward_s_event = SimEvent {
                        event: TriggerEvent::TunnelSent,
                        time: s_event.time,
                        packet_id: s_event.packet_id,
                        node_id: s_event.node_id,
                        link_id: s_event.link_id,
                        contains_padding: false,
                        bypass: s_event.bypass,
                        replace: s_event.replace,
                        q_sequence_nr: 0, // Will be overwritten by push()
                        #[cfg(debug_assertions)]
                        debug_note: None,
                    };
                    // Use blocking-aware logic to decide whether to queue immediately or block
                    maybenot_handle_tunnel_sent_creation(self, forward_s_event, sq);
                } else {
                    panic!(
                        "RelayMaybenot received NormalRecv on unexpected link index: {}",
                        s_event.link_id
                    );
                }
            }

            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time,
                    packet_id: s_event.packet_id,
                    node_id: s_event.node_id,
                    link_id: s_event.link_id,
                    contains_padding: true,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                // Use blocking-aware logic to decide whether to queue immediately or block
                maybenot_handle_tunnel_sent_creation(self, forward_s_event, sq);
            }

            TriggerEvent::TunnelSent => {
                crate::topology::nodes::make_network_receive_from_sent(
                    s_event, topology, linkstate, si, sq,
                );
            }

            TriggerEvent::BlockingEnd => {
                let mut state = self.sim_state.borrow_mut();
                // Release any queued events with current time
                maybenot_release_blocked_events(
                    self,
                    sq,
                    s_event.time,
                    state.drain_blocked_by_time,
                );

                // Clear blocking state
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }
}

/// A Maybenot-enabled relay+endpoint combined node that receives traffic and
/// responds with defenses.
///
/// # Network Topology Directionality
///
/// This node type combines relay and endpoint functionality. It sits at the
/// end of the network path (core side) and only handles traffic in the return
/// direction:
/// - **edgeside_in**: Receives traffic from the coreside (from prior hops)
/// - **edgeside_out**: Sends response traffic and defense actions back toward
///   the client
///
/// Traffic flow for a relay-endpoint:
/// ```text
/// Client --> ... --> RelayMaybenotEndpoint (receives on edgeside_in)
/// Client <-[edgeside_out]-- RelayMaybenotEndpoint (sends response + defense)
/// ```
///
/// # Defense Action Link
///
/// For RelayMaybenotEndpoint, all Maybenot defense actions (padding,
/// blocking) are sent on the **edgeside_out** link (back toward the client).
/// This is returned by `get_action_link_id()`.
///
/// # Implementation Note
///
/// Unlike EndpointBasic, this node includes an internal
/// `endpoint_prop_us` delay to simulate processing time at the endpoint
/// before responding.
#[derive(Debug, Clone)]
pub struct RelayMaybenotEndpoint {
    pub id: usize,
    /// Link ID for receiving traffic from prior network hops (forward
    /// direction)
    pub edgeside_in: usize,
    /// Link ID for sending response traffic and defense actions back toward the
    /// client (return direction)
    pub edgeside_out: usize,
    pub sim_state: RefCell<MaybenotState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimEvent>>,
    pub queue_normal: RefCell<VecDeque<SimEvent>>,
    /// Propagation delay simulating endpoint processing time
    pub endpoint_prop_us: Duration,
}

impl MaybenotNode for RelayMaybenotEndpoint {
    fn get_sim_state(&self) -> &RefCell<MaybenotState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }

    fn node_id(&self) -> usize {
        self.id
    }

    fn get_action_link_id(&self) -> usize {
        self.edgeside_out
    }

    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimEvent>> {
        &self.queue_padding
    }

    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimEvent>> {
        &self.queue_normal
    }

    fn trigger_update(
        &self,
        s_event: &SimEvent,
        current_time: &Instant,
        sq: &mut SimQueue,
        topology: &NetworkTopology,
    ) {
        maybenot_trigger_update(self, s_event, current_time, sq, topology)
    }

    fn do_internal_timer(&self, target: Instant) -> Option<SimEvent> {
        maybenot_do_internal_timer(self, target)
    }

    fn do_scheduled_action(&self, target: Instant) -> Option<SimEvent> {
        maybenot_do_scheduled_action(self, target)
    }
}

impl RelayMaybenotEndpoint {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        edgeside_in: usize,
        edgeside_out: usize,
        machines: Vec<Machine>,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        drain_blocked_by_time: bool,
        integration: Option<Integration>,
        insecure_rng_seed: Option<u64>,
        endpoint_prop_us: Duration,
    ) -> Self {
        let sim_state = RefCell::new(MaybenotState::new(
            machines,
            Instant::now(),
            max_padding_frac,
            max_blocking_frac,
            drain_blocked_by_time,
            integration,
            insecure_rng_seed,
        ));

        Self {
            id,
            edgeside_in,
            edgeside_out,
            sim_state,
            queue_padding: RefCell::new(VecDeque::new()),
            queue_normal: RefCell::new(VecDeque::new()),
            endpoint_prop_us,
        }
    }

    pub fn handle_event(
        &self,
        s_event: &SimEvent,
        topology: &NetworkTopology,
        linkstate: &mut NetworkLinkState,
        si: &SimInfo,
        sq: &mut SimQueue,
    ) {
        match &s_event.event {
            TriggerEvent::TunnelRecv => {
                let new_event = match &s_event.contains_padding {
                    true => TriggerEvent::PaddingRecv,
                    false => TriggerEvent::NormalRecv,
                };
                let forward_event = SimEvent {
                    event: new_event,
                    time: s_event.time,
                    packet_id: s_event.packet_id,
                    node_id: s_event.node_id,
                    link_id: s_event.link_id,
                    contains_padding: false,
                    bypass: false,
                    replace: false,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_event);
            }

            TriggerEvent::NormalRecv => {
                debug!(
                    "\tqueue {:#?} tx_depend check RelayMaybenotEndpoint",
                    TriggerEvent::NormalRecv
                );
                let mut timeadjusted_event = s_event.clone();
                timeadjusted_event.time += self.endpoint_prop_us; // Add delay to endpoint
                let outgoing_link = &linkstate.links[self.edgeside_out];
                check_dependent_packets(
                    &timeadjusted_event,
                    si,
                    sq,
                    outgoing_link,
                    self.endpoint_prop_us.as_micros() as u64,
                );
            }

            TriggerEvent::NormalSent => {
                // Only handle edgeside NormalSent - convert to TunnelSent with
                // blocking logic
                if s_event.link_id == self.edgeside_out {
                    let forward_s_event = SimEvent {
                        event: TriggerEvent::TunnelSent,
                        time: s_event.time,
                        packet_id: s_event.packet_id,
                        node_id: s_event.node_id,
                        link_id: s_event.link_id,
                        contains_padding: false,
                        bypass: s_event.bypass,
                        replace: s_event.replace,
                        q_sequence_nr: 0, // Will be overwritten by push()
                        #[cfg(debug_assertions)]
                        debug_note: None,
                    };
                    // Use blocking-aware logic to decide whether to queue
                    // immediately or block
                    maybenot_handle_tunnel_sent_creation(self, forward_s_event, sq);
                }
                // Ignore coreside NormalSent (shouldn't happen)
            }

            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time,
                    packet_id: s_event.packet_id,
                    node_id: s_event.node_id,
                    link_id: s_event.link_id,
                    contains_padding: true,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                // Use blocking-aware logic to decide whether to queue
                // immediately or block
                maybenot_handle_tunnel_sent_creation(self, forward_s_event, sq);
            }

            TriggerEvent::TunnelSent => {
                crate::topology::nodes::make_network_receive_from_sent(
                    s_event, topology, linkstate, si, sq,
                );
            }

            TriggerEvent::BlockingEnd => {
                let mut state = self.sim_state.borrow_mut();
                // Release any queued events with current time
                maybenot_release_blocked_events(
                    self,
                    sq,
                    s_event.time,
                    state.drain_blocked_by_time,
                );

                // Clear blocking state
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }
}
