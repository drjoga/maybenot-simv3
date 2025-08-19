use maybenot::{TriggerEvent, Machine, Framework, TriggerAction};
use crate::nodes::check_dependent_packets;
use crate::{SimulEvent, SimulInfo, SimulQueue};
use crate::topology::{NetworkTopology, NetworkLinkstate};
use crate::mbn_helpers::{mbn_trigger_update, mbn_do_internal_timer, mbn_do_scheduled_action};
use crate::integration::Integration;
use std::time::{Instant, Duration};
use std::cell::RefCell;
use std::collections::VecDeque;
use log::debug;

use rand::{rngs::ThreadRng, RngCore};
use rand_xoshiro::rand_core::SeedableRng;
use rand_xoshiro::Xoshiro256StarStar;





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

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        match self {
            RngSource::Thread(rng) => rng.try_fill_bytes(dest),
            RngSource::Xoshiro(rng) => rng.try_fill_bytes(dest),
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
#[derive(Debug,Clone)]
pub struct MbnState<M, R> {
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
    /// whether to drain blocked packets by time or first all normal then padding
    pub drain_blocked_by_time: bool,
    /// integration aspects for this state
    pub integration: Option<Integration>,
}

impl<M> MbnState<M, RngSource>
where
    M: AsRef<[Machine]>,
{
    pub fn new(
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
            None => RngSource::Thread(rand::thread_rng()),
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

    
    pub fn reporting_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(|i| i.reporting_delay())
            .unwrap_or(Duration::from_micros(0))
    }

    pub fn action_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(|i| i.action_delay())
            .unwrap_or(Duration::from_micros(0))
    }

    pub fn trigger_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(|i| i.trigger_delay())
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
pub fn mbn_handle_tunnel_sent_creation<T: MBNNode>(
    node: &T,
    s_event: SimulEvent,
    sq: &mut SimulQueue,
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
                if s_event.contains_padding  {
                    if s_event.replace {
                        // Check if we have a normal packet queued up
                        let mut normal_queue = node.get_queue_normal().borrow_mut();

                        if let Some(mut dequeued_normal_event) = normal_queue.pop_front() {
                            dequeued_normal_event.time = s_event.time; 
                            dequeued_normal_event.bypass = true;
                            debug!("Replacing bypass padding with normal event: {:?}", dequeued_normal_event);
                            sq.push(dequeued_normal_event);
                            return;
                        }   
                        else {
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
                    // If padding_replace and there is a blocked normal packet the padding is replaced, i.e. not enqueued
                    debug!("Padding replaced by blocked normal packet, nothing enqueued");
                    return;
                } else  {
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
    // Not blocking or past blocking time or bypass fallthrough - add to simulation queue immediately
    debug!("TunnelSent immediately");
    sq.push(s_event);
}

// Releases all queued events when a blocking period expires.
//
// Two drainage strategies are supported:
// 1. Time-ordered: Events drain in chronological order by original timestamp
// 2. Type-ordered: All normal packets first, then all padding packets
pub fn mbn_release_blocked_events<T: MBNNode>(
    node: &T,
    sq: &mut SimulQueue,
    current_time: Instant,
    drain_blocked_by_time: bool,
) {
    // Release all events from both queues
    let mut padding_events = node.get_queue_padding().borrow_mut();
    let mut normal_events = node.get_queue_normal().borrow_mut();
    
    debug!("Releasing {} padding events and {} normal events", 
           padding_events.len(), normal_events.len());
    

    if drain_blocked_by_time {
        // Time-wise draining: release packets in chronological order based on their original timestamps
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
                    debug!("Releasing padding event (time-wise): {:?} originally at {:?}, now at {:?}", 
                           event.event, event.time, current_time);
                    event.time = current_time;
                    sq.push(event);
                }
            } else if let Some(mut event) = normal_events.pop_front() {
                debug!("Releasing normal event (time-wise): {:?} originally at {:?}, now at {:?}", 
                       event.event, event.time, current_time);
                event.time = current_time;
                sq.push(event);
            }
        }
    } else {
        // Move all normal queue events to simulation queue with updated time
        for mut event in normal_events.drain(..) {
            debug!("Releasing normal event: {:?} originally at {:?}, now at {:?}", 
                event.event, event.time, current_time);
            event.time = current_time;
            sq.push(event);
        }

        // Move all padding queue events to simulation queue with updated time
        for mut event in padding_events.drain(..) {
            debug!("Releasing padding event: {:?} originally at {:?}, now at {:?}", 
                event.event, event.time, current_time);
            event.time = current_time;
            sq.push(event);
        }
    }
}


// Trait for MBN nodes to enable generic implementations
pub trait MBNNode {
    fn get_sim_state(&self) -> &RefCell<MbnState<Vec<Machine>, RngSource>>;
    fn node_id(&self) -> usize;
    fn get_action_link_id(&self) -> usize; // Link used for actions (coreside for client, edgeside for relay)
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>>;
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>>;
    
    // Methods needed for simulation
    fn trigger_update(&self, s_event: &SimulEvent, current_time: &Instant, sq: &mut SimulQueue, topology: &NetworkTopology);
    fn do_internal_timer(&self, target: Instant) -> Option<SimulEvent>;
    fn do_scheduled_action(&self, target: Instant) -> Option<SimulEvent>;
}


#[derive(Debug, Clone)]
pub struct ClientMBN {
    pub id: usize,
    pub coreside_out: usize,
    pub sim_state: RefCell<MbnState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
}

impl MBNNode for ClientMBN {
    fn get_sim_state(&self) -> &RefCell<MbnState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }
    
    fn node_id(&self) -> usize {
        self.id
    }
    
    fn get_action_link_id(&self) -> usize {
        self.coreside_out
    }
    
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_padding
    }
    
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_normal
    }

    fn trigger_update(&self, s_event: &SimulEvent, current_time: &Instant, sq: &mut SimulQueue, topology: &NetworkTopology) {
        mbn_trigger_update(self, s_event, current_time, sq, topology)
    }
    
    fn do_internal_timer(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_internal_timer(self, target)
    }
    
    fn do_scheduled_action(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_scheduled_action(self, target)
    }
}

#[allow(clippy::too_many_arguments)]
impl ClientMBN {
    pub fn new(
        id: usize, 
        coreside_out: usize,
        machines: Vec<Machine>,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        drain_blocked_by_time: bool,
        integration: Option<Integration>,
        insecure_rng_seed: Option<u64>
    ) -> Self {
        let sim_state = RefCell::new(MbnState::new(
            machines,
            Instant::now(),
            max_padding_frac,
            max_blocking_frac,
            drain_blocked_by_time,
            integration,
            insecure_rng_seed
        ));
        
        Self {
            id,
            coreside_out,
            sim_state,
            queue_padding: RefCell::new(VecDeque::new()),
            queue_normal: RefCell::new(VecDeque::new()),
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalSent => {
                let forward_s_event = SimulEvent {
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
                mbn_handle_tunnel_sent_creation(self, forward_s_event,  sq);
            }

            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimulEvent {
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
                mbn_handle_tunnel_sent_creation(self, forward_s_event,sq);
            }

            TriggerEvent::TunnelSent => {
                crate::nodes::make_network_receive_from_sent(s_event, topology, linkstate, si, sq);
            }

            TriggerEvent::TunnelRecv => {
                let new_t_event = match &s_event.contains_padding {
                    true => {
                        TriggerEvent::PaddingRecv
                    },
                    false => {
                        TriggerEvent::NormalRecv
                    }
                };
                let forward_s_event = SimulEvent {
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

                crate::nodes::check_dependent_packets(s_event, si,sq, outgoing_link, 0);
            }

            TriggerEvent::BlockingEnd => {
                let mut state = self.sim_state.borrow_mut();
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, s_event.time, state.drain_blocked_by_time);
                
                // Clear blocking state
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelayMBN {
    pub id: usize,
    pub coreside_out: usize,
    pub edgeside_in: usize,
    pub edgeside_out: usize,
    pub sim_state: RefCell<MbnState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
}

impl MBNNode for RelayMBN {
    fn get_sim_state(&self) -> &RefCell<MbnState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }
    
    fn node_id(&self) -> usize {
        self.id
    }
    
    fn get_action_link_id(&self) -> usize {
        self.edgeside_out
    }
    
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_padding
    }
    
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_normal
    }
    
    fn trigger_update(&self, s_event: &SimulEvent, current_time: &Instant, sq: &mut SimulQueue, topology: &NetworkTopology) {
        mbn_trigger_update(self, s_event, current_time, sq, topology)
    }
    
    fn do_internal_timer(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_internal_timer(self, target)
    }
    
    fn do_scheduled_action(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_scheduled_action(self, target)
    }
}

impl RelayMBN {
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
        insecure_rng_seed: Option<u64>
    ) -> Self {
        let sim_state = RefCell::new(MbnState::new(
            machines,
            Instant::now(),
            max_padding_frac,
            max_blocking_frac,
            drain_blocked_by_time,
            integration,
            insecure_rng_seed
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

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::TunnelRecv => {
                let new_event = match &s_event.contains_padding {
                    true => {
                        TriggerEvent::PaddingRecv
                    },
                    false => {
                        TriggerEvent::NormalRecv
                    }
                };
                let forward_event = SimulEvent {
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
                let outlink = topology.get_outlink(s_event.node_id, s_event.link_id).unwrap();
                if  outlink == self.coreside_out {
                    crate::nodes::forward_network_receive_from_receive(s_event, topology, linkstate, si, sq);
                } else if outlink == self.edgeside_out {
                    let new_s_event = SimulEvent {
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
                    panic!("RelayMBN received NormalRecv on unexpected link index: {}", s_event.link_id);
                }
            }

            TriggerEvent::NormalSent => {
                if  s_event.link_id == self.coreside_out {
                    crate::nodes::make_network_receive_from_sent(s_event, topology, linkstate, si, sq);
                } else if s_event.link_id == self.edgeside_out {
                    let forward_s_event = SimulEvent {
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
                    mbn_handle_tunnel_sent_creation(self, forward_s_event, sq);
                } else {
                    panic!("RelayMBN received NormalRecv on unexpected link index: {}", s_event.link_id);
                }
            }

            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimulEvent {
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
                mbn_handle_tunnel_sent_creation(self, forward_s_event, sq);
            }

            TriggerEvent::TunnelSent => {
                crate::nodes::make_network_receive_from_sent(s_event, topology, linkstate, si, sq);
            }

            TriggerEvent::BlockingEnd => {
                let mut state = self.sim_state.borrow_mut();
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, s_event.time, state.drain_blocked_by_time);
                
                // Clear blocking state
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }

}

#[derive(Debug, Clone)]
pub struct RelayMBNtserver {
    pub id: usize,
    pub edgeside_in: usize,
    pub edgeside_out: usize,
    pub sim_state: RefCell<MbnState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
    pub ts_prop_us: Duration,
}

impl MBNNode for RelayMBNtserver {
    fn get_sim_state(&self) -> &RefCell<MbnState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }
    
    fn node_id(&self) -> usize {
        self.id
    }
    
    fn get_action_link_id(&self) -> usize {
        self.edgeside_out
    }
    
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_padding
    }
    
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_normal
    }

    fn trigger_update(&self, s_event: &SimulEvent, current_time: &Instant, sq: &mut SimulQueue, topology: &NetworkTopology) {
        mbn_trigger_update(self, s_event, current_time, sq, topology)
    }
    
    fn do_internal_timer(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_internal_timer(self, target)
    }
    
    fn do_scheduled_action(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_scheduled_action(self, target)
    }
}

impl RelayMBNtserver {
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
        ts_prop_us: Duration,
    ) -> Self {
        let sim_state = RefCell::new(MbnState::new(
            machines,
            Instant::now(),
            max_padding_frac,
            max_blocking_frac,
            drain_blocked_by_time,
            integration,
            insecure_rng_seed
        ));
        
        Self {
            id,
            edgeside_in,
            edgeside_out,
            sim_state,
            queue_padding: RefCell::new(VecDeque::new()),
            queue_normal: RefCell::new(VecDeque::new()),
            ts_prop_us,
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::TunnelRecv => {
                let new_event = match &s_event.contains_padding {
                    true => {
                        TriggerEvent::PaddingRecv
                    },
                    false => {
                        TriggerEvent::NormalRecv
                    }
                };
                let forward_event = SimulEvent {
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
                debug!("\tqueue {:#?} tx_depend check RelayMBNtserver", TriggerEvent::NormalRecv);
                let mut timeadjusted_event = s_event.clone();
                timeadjusted_event.time += self.ts_prop_us; // Add delay to trafficserver
                let outgoing_link = &linkstate.links[self.edgeside_out];
                check_dependent_packets(&timeadjusted_event, si,sq, outgoing_link, self.ts_prop_us.as_micros()  as u64);
            }

            TriggerEvent::NormalSent => {
                // Only handle edgeside NormalSent - convert to TunnelSent with blocking logic
                if s_event.link_id == self.edgeside_out {
                    let forward_s_event = SimulEvent {
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
                    mbn_handle_tunnel_sent_creation(self, forward_s_event, sq);
                }
                // Ignore coreside NormalSent (shouldn't happen)
            }

            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimulEvent {
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
                mbn_handle_tunnel_sent_creation(self, forward_s_event, sq);
            }

            TriggerEvent::TunnelSent => {
                crate::nodes::make_network_receive_from_sent(s_event, topology, linkstate, si, sq);
            }

            TriggerEvent::BlockingEnd => {
                let mut state = self.sim_state.borrow_mut();
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, s_event.time, state.drain_blocked_by_time);

                // Clear blocking state
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }

}