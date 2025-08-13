use maybenot::{TriggerEvent, Machine};
use crate::nodes::check_dependent_packets;
use crate::{SimulEvent, SimulInfo, SimulQueue, SimState, RngSource};
use crate::topology::{NetworkTopology, NetworkLinkstate};
use crate::mbn_helpers::{mbn_trigger_update, mbn_do_internal_timer, mbn_do_scheduled_action};
use std::time::{Instant, Duration};
use std::cell::RefCell;
use std::collections::VecDeque;
use log::debug;



// Helper function to handle TunnelSent event creation with blocking logic
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
                        debug!("Sending bypass padding");  
                    }
                } else {
                    debug!("Sending bypass Normal packet");
                }
            } else if s_event.contains_padding {
                    node.get_queue_padding().borrow_mut().push_back(s_event);
                    debug!("Blocking Padding enqued");
                    return;
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

// Helper function to release queued events when blocking ends
pub fn mbn_release_blocked_events<T: MBNNode>(
    node: &T,
    sq: &mut SimulQueue,
    current_time: Instant,
) {
    // Release all events from both queues
    let mut padding_events = node.get_queue_padding().borrow_mut();
    let mut normal_events = node.get_queue_normal().borrow_mut();
    
    debug!("Releasing {} padding events and {} normal events", 
           padding_events.len(), normal_events.len());
    
    // Move all padding queue events to simulation queue with updated time
    for mut event in padding_events.drain(..) {
        debug!("Releasing padding event: {:?} originally at {:?}, now at {:?}", 
               event.event, event.time, current_time);
        event.time = current_time;
        sq.push(event);
    }
    
    // Move all normal queue events to simulation queue with updated time
    for mut event in normal_events.drain(..) {
        debug!("Releasing normal event: {:?} originally at {:?}, now at {:?}", 
               event.event, event.time, current_time);
        event.time = current_time;
        sq.push(event);
    }
}


// Trait for MBN nodes to enable generic implementations
pub trait MBNNode {
    fn get_sim_state(&self) -> &RefCell<SimState<Vec<Machine>, RngSource>>;
    fn node_id(&self) -> usize;
    fn get_action_link_id(&self) -> usize; // Link used for actions (coreside for client, edgeside for relay)
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>>;
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>>;
    
    // Methods needed for simulation
    fn trigger_update(&self, s_event: &SimulEvent, current_time: &Instant, sq: &mut SimulQueue, topology: &NetworkTopology);
    fn do_internal_timer(&self, target: Instant) -> Option<SimulEvent>;
    fn do_scheduled_action(&self, target: Instant) -> Option<SimulEvent>;
}


#[derive(Debug)]
pub struct ClientMBN {
    pub id: usize,
    pub coreside_out: usize,
    pub sim_state: RefCell<SimState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
}

impl MBNNode for ClientMBN {
    fn get_sim_state(&self) -> &RefCell<SimState<Vec<Machine>, RngSource>> {
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

impl ClientMBN {
    pub fn new(
        id: usize, 
        coreside_out: usize,
        machines: Vec<Machine>,
        current_time: Instant,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        insecure_rng_seed: Option<u64>
    ) -> Self {
        let sim_state = RefCell::new(SimState::new(
            machines,
            current_time,
            max_padding_frac,
            max_blocking_frac,
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
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, s_event.time);
                
                // Clear blocking state
                let mut state = self.sim_state.borrow_mut();
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct RelayMBN {
    pub id: usize,
    pub coreside_out: usize,
    pub edgeside_in: usize,
    pub edgeside_out: usize,
    pub sim_state: RefCell<SimState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
}

impl MBNNode for RelayMBN {
    fn get_sim_state(&self) -> &RefCell<SimState<Vec<Machine>, RngSource>> {
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
        current_time: Instant,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        insecure_rng_seed: Option<u64>
    ) -> Self {
        let sim_state = RefCell::new(SimState::new(
            machines,
            current_time,
            max_padding_frac,
            max_blocking_frac,
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
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, s_event.time);
                
                // Clear blocking state
                let mut state = self.sim_state.borrow_mut();
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }

}

#[derive(Debug)]
pub struct RelayMBNtserver {
    pub id: usize,
    pub edgeside_in: usize,
    pub edgeside_out: usize,
    pub sim_state: RefCell<SimState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
    pub ts_prop_us: Duration,
}

impl MBNNode for RelayMBNtserver {
    fn get_sim_state(&self) -> &RefCell<SimState<Vec<Machine>, RngSource>> {
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
        current_time: Instant,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        insecure_rng_seed: Option<u64>,
        ts_prop_us: Duration,
    ) -> Self {
        let sim_state = RefCell::new(SimState::new(
            machines,
            current_time,
            max_padding_frac,
            max_blocking_frac,
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
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, s_event.time);
                
                // Clear blocking state
                let mut state = self.sim_state.borrow_mut();
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            _ => {}
        }
    }

}