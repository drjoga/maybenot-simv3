use maybenot::{TriggerEvent, Machine, TriggerAction, Timer, MachineId};
use crate::{SimulEvent, SimulQueue, SimState, ScheduledAction, SimulatorArgs};
use crate::network::NetworkTopology;
use crate::mbn_nodes::MBNNode;
use std::time::{Duration, Instant};
use log::debug;



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


/// Initialize MBN nodes with SimState for simulation
pub fn initialize_mbn_sim_states(
    topology: &NetworkTopology,
    machines_client: &[Machine],
    machines_server: &[Machine],
    current_time: Instant,
    args: &SimulatorArgs,
) {
    // Initialize client MBN node using trait abstraction
    let client_mbn = topology.get_mbn_client();
    let new_state = SimState::new(
        machines_client.to_vec(),
        current_time,
        args.max_padding_frac_client,
        args.max_blocking_frac_client,
        args.insecure_rng_seed,
    );
    *client_mbn.get_sim_state().borrow_mut() = new_state;

    // Initialize server MBN node using trait abstraction
    let relay_mbn = topology.get_mbn_server();
    let new_state = SimState::new(
        machines_server.to_vec(),
        current_time,
        args.max_padding_frac_server,
        args.max_blocking_frac_server,
        args.insecure_rng_seed.map(|seed| seed.wrapping_add(1)),
    );
    *relay_mbn.get_sim_state().borrow_mut() = new_state;
}



// Generic helper functions for MBN operations
pub fn mbn_trigger_update<T: MBNNode>(
    node: &T,
    s_event: &SimulEvent,
    current_time: &Instant,
    sq: &mut SimulQueue,
    _topology: &NetworkTopology
) {
    let node_idx = node.node_id();
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
        node_idx: node.node_id(),
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
    target: Instant
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
                node_idx: node.node_id(),
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

            if replace || block > state.blocking_until.unwrap_or(a.time) {
                state.blocking_until = Some(block);
                state.blocking_bypassable = bypass;
                // BlockingEnd events are generated by the main simulation loop
                // to ensure proper timing relative to other events
            }
            let event_bypass = state.blocking_bypassable;

            Some(SimulEvent {
                event: TriggerEvent::BlockingBegin { machine },
                time: a.time,
                packet_idx: usize::MAX,
                node_idx: node.node_id(),
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
