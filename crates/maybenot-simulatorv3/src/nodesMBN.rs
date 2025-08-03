use maybenot::{TriggerEvent, Machine, TriggerAction, Timer, MachineId};
use crate::{SimulEvent, SimulQueue, SimState, RngSource, ScheduledAction};
use crate::network::{NetworkTopology, NetworkLinkstate};
use std::time::{Duration, Instant};
use std::cell::RefCell;
use std::collections::VecDeque;
use log::debug;

// Trait for MBN nodes to enable generic implementations
pub trait MBNNode {
    fn get_sim_state(&self) -> &RefCell<SimState<Vec<Machine>, RngSource>>;
    fn get_node_id(&self) -> usize;
    fn get_action_link_id(&self) -> usize; // Link used for actions (coreside for client, edgeside for relay)
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>>;
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>>;
}



pub fn peek_scheduled_action(
    scheduled_c: &[Option<ScheduledAction>],
    scheduled_s: &[Option<ScheduledAction>],
    current_time: Instant,
) -> Duration {
    // there are at most one scheduled action per machine, so we can just
    // iterate over all of them quickly
    let mut earliest = Duration::MAX;

    for a in scheduled_c.iter().flatten() {
        if a.time >= current_time && a.time.duration_since(current_time) < earliest {
            earliest = a.time.duration_since(current_time);
        }
    }
    for a in scheduled_s.iter().flatten() {
        if a.time >= current_time && a.time.duration_since(current_time) < earliest {
            earliest = a.time.duration_since(current_time);
        }
    }

    earliest
}

pub fn peek_scheduled_internal_timer(
    internal_c: &[Option<Instant>],
    internal_s: &[Option<Instant>],
    current_time: Instant,
) -> Duration {
    // there are at most one internal event per machine, so we can just
    // iterate over all of them quickly
    let mut earliest = Duration::MAX;

    for t in internal_c.iter().flatten() {
        if *t >= current_time && t.duration_since(current_time) < earliest {
            earliest = t.duration_since(current_time);
        }
    }
    for t in internal_s.iter().flatten() {
        if *t >= current_time && t.duration_since(current_time) < earliest {
            earliest = t.duration_since(current_time);
        }
    }

    earliest
}

pub fn peek_blocked_exp(
    blocking_c: Option<Instant>,
    blocking_s: Option<Instant>,
    current_time: Instant,
) -> (Duration, bool) {
    match (blocking_c, blocking_s) {
        (Some(c), Some(s)) => {
            if c < s {
                (c.duration_since(current_time), true)
            } else {
                (s.duration_since(current_time), false)
            }
        }
        (Some(c), None) => (c.duration_since(current_time), true),
        (None, Some(s)) => (s.duration_since(current_time), false),
        (None, None) => (Duration::MAX, true),
    }
}


// Helper function to handle TunnelSent event creation with blocking logic
pub fn mbn_handle_tunnel_sent_creation<T: MBNNode>(
    node: &T,
    s_event: &SimulEvent,
    sq: &mut SimulQueue,
    topology: &NetworkTopology,
    linkstate: &mut NetworkLinkstate,
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
            } else {
                if s_event.contains_padding {
                    node.get_queue_padding().borrow_mut().push_back(s_event.clone());
                    debug!("Blocking Padding enqued");
                    return;
                } else {
                    node.get_queue_normal().borrow_mut().push_back(s_event.clone());
                    debug!("Blocking Normal enqued");
                    return;
                }
            }
        }
    }
    // Not blocking or past blocking time or bypass fallthrough - add to simulation queue immediately
    debug!("TunnelSent immediately");
    sq.push(s_event.clone());
}

// Helper function to release queued events when blocking ends
pub fn mbn_release_blocked_events<T: MBNNode>(
    node: &T,
    sq: &mut SimulQueue,
    topology: &NetworkTopology,
    linkstate: &mut NetworkLinkstate,
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

// Generic helper functions for MBN operations
pub fn mbn_trigger_update<T: MBNNode>(
    node: &T,
    s_event: &SimulEvent,
    current_time: &Instant,
    sq: &mut SimulQueue,
    _topology: &NetworkTopology
) {
    let node_idx = node.get_node_id();
    let link_idx = node.get_action_link_id();

    // Clone the actions to avoid borrowing issues
    let actions: Vec<_> = {
        let mut state = node.get_sim_state().borrow_mut();
        state
            .framework
            .trigger_events(&[s_event.event.clone()], *current_time)
            .cloned()
            .collect()
    };
    
    // Now process actions with a fresh borrow
    for action in actions {
        let mut state = node.get_sim_state().borrow_mut();
        match action {
            TriggerAction::Cancel { machine, timer } => {
                debug!(
                    "\ttrigger_update(): cancel action {:?} {:?}",
                    machine, timer
                );
                // here we make a simplifying assumption of no trigger delay for
                // cancel actions
                match timer {
                    Timer::Action => {
                        state.scheduled_action[machine.into_raw()] = None;
                    }
                    Timer::Internal => {
                        state.scheduled_internal_timer[machine.into_raw()] = None;
                    }
                    Timer::All => {
                        state.scheduled_action[machine.into_raw()] = None;
                        state.scheduled_internal_timer[machine.into_raw()] = None;
                    }
                }
            }
            TriggerAction::SendPadding {
                timeout,
                bypass: _,
                replace: _,
                machine,
            } => {
                debug!(
                    "\ttrigger_update(): send padding action {:?} {:?}",
                    timeout, machine
                );
                state.scheduled_action[machine.into_raw()] = Some(ScheduledAction {
                    action: action.clone(),
                    time: *current_time + timeout,
                });
            }
            TriggerAction::BlockOutgoing {
                timeout,
                duration: _,
                bypass: _,
                replace: _,
                machine,
            } => {
                debug!(
                    "\ttrigger_update(): block outgoing action {:?} {:?}",
                    timeout, machine
                );
                state.scheduled_action[machine.into_raw()] = Some(ScheduledAction {
                    action: action.clone(),
                    time: *current_time + timeout,
                });
            }
            TriggerAction::UpdateTimer {
                duration,
                replace,
                machine,
            } => {
                debug!(
                    "\ttrigger_update(): update timer action {:?} {:?}",
                    duration, machine
                );
                // get current internal timer duration, if any
                let current =
                    state.scheduled_internal_timer[machine.into_raw()].unwrap_or(*current_time);

                // update the timer
                if replace || current < *current_time + duration {
                    state.scheduled_internal_timer[machine.into_raw()] =
                        Some(*current_time + duration);
                    // TimerBegin event
                    sq.push(SimulEvent {
                        event: TriggerEvent::TimerBegin { machine },
                        time: *current_time,
                        packet_idx: usize::MAX,
                        node_idx,
                        link_idx,
                        bypass: false,
                        replace: false,
                        contains_padding: false,
                        q_sequence_nr: 0,
                        #[cfg(debug_assertions)]
                        debug_note: None,
                    });
                }
            }
        }
    }
}

pub fn mbn_do_internal_timer<T: MBNNode>(
    node: &T,
    target: Instant
) -> Option<SimulEvent> {
    let mut state = node.get_sim_state().borrow_mut();
    let mut machine: Option<MachineId> = None;

    for (id, opt) in state.scheduled_internal_timer.iter_mut().enumerate() {
        if let Some(a) = opt {
            if *a == target {
                machine = Some(MachineId::from_raw(id));
                *opt = None;
                break;
            }
        }
    }

    machine.map(|machine| SimulEvent {
        event: TriggerEvent::TimerEnd { machine },
        time: target,
        packet_idx: usize::MAX,
        node_idx: node.get_node_id(),
        link_idx: node.get_action_link_id(),
        bypass: false,
        replace: false,
        contains_padding: false,
        q_sequence_nr: 0,
        #[cfg(debug_assertions)]
        debug_note: None,
    })
}

pub fn mbn_do_scheduled_action<T: MBNNode>(
    node: &T,
    target: Instant,
    sq: &mut SimulQueue
) -> Option<SimulEvent> {
    let mut state = node.get_sim_state().borrow_mut();
    let mut a: Option<ScheduledAction> = None;

    for opt in state.scheduled_action.iter_mut() {
        if let Some(sa) = opt {
            if sa.time == target {
                a = Some(sa.clone());
                *opt = None;
                break;
            }
        }
    }

    let a = a?;

    match a.action {
        TriggerAction::Cancel { .. } => {
            panic!("BUG: cancel action in scheduled action");
        }
        TriggerAction::UpdateTimer { .. } => {
            panic!("BUG: update timer action in scheduled action");
        }
        TriggerAction::SendPadding {
            timeout: _,
            bypass,
            replace,
            machine,
        } => {
            Some(SimulEvent {
                event: TriggerEvent::PaddingSent { machine },
                time: a.time,
                packet_idx: usize::MAX,
                node_idx: node.get_node_id(),
                link_idx: node.get_action_link_id(),
                bypass,
                replace,
                contains_padding: true,
                q_sequence_nr: 0,
                #[cfg(debug_assertions)]
                debug_note: None,
            })
        }
        TriggerAction::BlockOutgoing {
            timeout: _,
            duration,
            bypass,
            replace,
            machine,
        } => {
            let block = a.time + duration;
            let event_bypass;

            if replace || block > state.blocking_until.unwrap_or(a.time) {
                state.blocking_until = Some(block);
                state.blocking_bypassable = bypass;
                // BlockingEnd events are generated by the main simulation loop
                // to ensure proper timing relative to other events
            }
            event_bypass = state.blocking_bypassable;

            Some(SimulEvent {
                event: TriggerEvent::BlockingBegin { machine },
                time: a.time,
                packet_idx: usize::MAX,
                node_idx: node.get_node_id(),
                link_idx: node.get_action_link_id(),
                bypass: event_bypass,
                replace: false,
                contains_padding: false,
                q_sequence_nr: 0,
                #[cfg(debug_assertions)]
                debug_note: None,
            })
        }
    }
}

// MBN (Maybenot) node types - initially behave like Basic nodes but designed for future MBN integration

#[derive(Debug)]
pub struct ClientMBN {
    pub id: usize,
    coreside_link: usize,
    pub sim_state: RefCell<SimState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
}

impl MBNNode for ClientMBN {
    fn get_sim_state(&self) -> &RefCell<SimState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }
    
    fn get_node_id(&self) -> usize {
        self.id
    }
    
    fn get_action_link_id(&self) -> usize {
        self.coreside_link
    }
    
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_padding
    }
    
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_normal
    }
}

impl ClientMBN {
    pub fn new(
        id: usize, 
        coreside_link: usize,
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
            coreside_link,
            sim_state,
            queue_padding: RefCell::new(VecDeque::new()),
            queue_normal: RefCell::new(VecDeque::new()),
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalSent => {
                let forward_s_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time, 
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
                    contains_padding: false,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                // Use blocking-aware logic to decide whether to queue immediately or block
                mbn_handle_tunnel_sent_creation(self, &forward_s_event, sq, topology, linkstate);
            }
            

            TriggerEvent::TunnelSent => {
                crate::nodes::make_network_receive_from_sent(s_event, topology, linkstate, sq);
            }


            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time, 
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
                    contains_padding: true,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                // Use blocking-aware logic to decide whether to queue immediately or block
                mbn_handle_tunnel_sent_creation(self, &forward_s_event, sq, topology, linkstate);
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
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
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
                let outgoing_link_id = topology.nodes[s_event.node_idx].get_coreside_linkid();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                crate::nodes::check_dependent_packets(s_event, sq, outgoing_link);
            }
            TriggerEvent::PaddingRecv => {}
            TriggerEvent::BlockingBegin { .. } => {
                // Blocking state is already updated in mbn_do_scheduled_action
            }
            TriggerEvent::BlockingEnd => {
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, topology, linkstate, s_event.time);
                
                // Clear blocking state
                let mut state = self.sim_state.borrow_mut();
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            TriggerEvent::TimerBegin { .. } => {
            }
            TriggerEvent::TimerEnd { .. }  => {
            }

            _ => {
                panic!("ClientMBN cannot handle s_event: {:?}", s_event.event);
            }
        }
    }

    pub fn trigger_update(
        &self, 
        s_event: &SimulEvent, 
        current_time: &Instant, 
        sq: &mut SimulQueue, 
        topology: &NetworkTopology
    ) {
        mbn_trigger_update(self, s_event, current_time, sq, topology)
    }

    pub fn do_internal_timer(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_internal_timer(self, target)
    }

    pub fn do_scheduled_action(&self, target: Instant, sq: &mut SimulQueue) -> Option<SimulEvent> {
        mbn_do_scheduled_action(self, target, sq)
    }

    pub fn node_id(&self) -> usize {
        self.id
    }

    pub fn get_coreside_linkid(&self) -> usize {
        self.coreside_link
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        panic!("ClientMBN does not have an edgeside link")
    }
}

#[derive(Debug)]
pub struct RelayMBN {
    pub id: usize,
    pub coreside_link: usize,
    pub edgeside_link: usize,
    pub sim_state: RefCell<SimState<Vec<Machine>, RngSource>>,
    pub queue_padding: RefCell<VecDeque<SimulEvent>>,
    pub queue_normal: RefCell<VecDeque<SimulEvent>>,
}

impl MBNNode for RelayMBN {
    fn get_sim_state(&self) -> &RefCell<SimState<Vec<Machine>, RngSource>> {
        &self.sim_state
    }
    
    fn get_node_id(&self) -> usize {
        self.id
    }
    
    fn get_action_link_id(&self) -> usize {
        self.edgeside_link
    }
    
    fn get_queue_padding(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_padding
    }
    
    fn get_queue_normal(&self) -> &RefCell<VecDeque<SimulEvent>> {
        &self.queue_normal
    }
}

impl RelayMBN {
    pub fn new(
        id: usize, 
        coreside_link: usize, 
        edgeside_link: usize,
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
            coreside_link,
            edgeside_link,
            sim_state,
            queue_padding: RefCell::new(VecDeque::new()),
            queue_normal: RefCell::new(VecDeque::new()),
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
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
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx,
                    link_idx: s_event.link_idx,
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
                let outlink = topology.get_outlink(s_event.node_idx, s_event.link_idx).unwrap();
                if  outlink == self.coreside_link {
                    crate::nodes::forward_network_receive_from_receive(s_event, topology, linkstate, sq);
                } else if outlink == self.edgeside_link {
                    let new_s_event = SimulEvent {
                        event: TriggerEvent::NormalSent,
                        time: s_event.time,
                        packet_idx: s_event.packet_idx,
                        node_idx: s_event.node_idx,
                        link_idx: outlink,
                        contains_padding: false,
                        bypass: false,
                        replace: false,
                        q_sequence_nr: 0, // Will be overwritten by push()
                        #[cfg(debug_assertions)]
                        debug_note: None, 
                    };
                    sq.push(new_s_event);
                } else {
                    panic!("RelayMBN received NormalRecv on unexpected link index: {}", s_event.link_idx);
                }
            }

            TriggerEvent::NormalSent => {
                if  s_event.link_idx == self.coreside_link {
                    crate::nodes::make_network_receive_from_sent(s_event, topology, linkstate, sq);
                } else if s_event.link_idx == self.edgeside_link {
                    let forward_s_event = SimulEvent {
                        event: TriggerEvent::TunnelSent,
                        time: s_event.time, 
                        packet_idx: s_event.packet_idx,
                        node_idx: s_event.node_idx, 
                        link_idx: s_event.link_idx, 
                        contains_padding: false,
                        bypass: s_event.bypass,
                        replace: s_event.replace,
                        q_sequence_nr: 0, // Will be overwritten by push()
                        #[cfg(debug_assertions)]
                        debug_note: None,
                    };
                    // Use blocking-aware logic to decide whether to queue immediately or block
                    mbn_handle_tunnel_sent_creation(self, &forward_s_event, sq, topology, linkstate);
                } else {
                    panic!("RelayMBN received NormalRecv on unexpected link index: {}", s_event.link_idx);
                }
            }


            TriggerEvent::TunnelSent => {
                crate::nodes::make_network_receive_from_sent(s_event, topology, linkstate, sq);
            }


            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time, 
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
                    contains_padding: true,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                // Use blocking-aware logic to decide whether to queue immediately or block
                mbn_handle_tunnel_sent_creation(self, &forward_s_event, sq, topology, linkstate);
            }
            TriggerEvent::PaddingRecv => {}
            TriggerEvent::BlockingBegin { .. } => {
                // Blocking state is already updated in mbn_do_scheduled_action
            }
            TriggerEvent::BlockingEnd => {
                // Release any queued events with current time
                mbn_release_blocked_events(self, sq, topology, linkstate, s_event.time);
                
                // Clear blocking state
                let mut state = self.sim_state.borrow_mut();
                state.blocking_until = None;
                state.blocking_bypassable = false;
            }
            TriggerEvent::TimerBegin { .. } => {
            }
            TriggerEvent::TimerEnd { .. }  => {
            }

            _ => {
                panic!("RelayMBN cannot handle s_event: {:?}", s_event.event);
            }
        }
    }

    pub fn trigger_update(
        &self, 
        s_event: &SimulEvent, 
        current_time: &Instant, 
        sq: &mut SimulQueue, 
        topology: &NetworkTopology
    ) {
        mbn_trigger_update(self, s_event, current_time, sq, topology)
    }

    pub fn do_internal_timer(&self, target: Instant) -> Option<SimulEvent> {
        mbn_do_internal_timer(self, target)
    }

    pub fn do_scheduled_action(&self, target: Instant, sq: &mut SimulQueue) -> Option<SimulEvent> {
        mbn_do_scheduled_action(self, target, sq)
    }

    pub fn node_id(&self) -> usize {
        self.id
    }

    pub fn get_coreside_linkid(&self) -> usize {
        self.coreside_link
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        self.edgeside_link
    }
}