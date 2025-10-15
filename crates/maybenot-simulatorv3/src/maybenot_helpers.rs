use crate::maybenot_nodes::MaybenotNode;
use crate::maybenot_nodes::{MaybenotState, ScheduledAction};
use crate::topology::NetworkTopology;
use crate::{SimEvent, SimInfo, SimQueue, SimulatorArgs};
use log::debug;
use maybenot::{Machine, MachineId, Timer, TriggerAction, TriggerEvent};
use std::time::{Duration, Instant};

/// Initialize Maybenot nodes with MaybenotState for simulation
pub fn initialize_maybenot_sim_states(
    topology: &NetworkTopology,
    machines_client: &[Machine],
    machines_server: &[Machine],
    current_time: Instant,
    args: &SimulatorArgs,
) {
    // Initialize client Maybenot node using trait abstraction
    let client_maybenot: &dyn MaybenotNode = topology.get_maybenot_client();
    let new_state = MaybenotState::new(
        machines_client.to_vec(),
        current_time,
        args.max_padding_frac_client,
        args.max_blocking_frac_client,
        args.drain_blocked_by_time,
        args.client_integration.clone(),
        args.insecure_rng_seed,
    );
    *client_maybenot.get_sim_state().borrow_mut() = new_state;

    // Initialize relay Maybenot node using trait abstraction
    let relay_maybenot: &dyn MaybenotNode = topology.get_maybenot_server();
    let new_state = MaybenotState::new(
        machines_server.to_vec(),
        current_time,
        args.max_padding_frac_server,
        args.max_blocking_frac_server,
        args.drain_blocked_by_time,
        args.server_integration.clone(),
        // if we have an insecure seed, we use the next number in the sequence
        // to avoid the same seed for both client and server
        args.insecure_rng_seed.map(|seed| seed.wrapping_add(1)),
    );
    *relay_maybenot.get_sim_state().borrow_mut() = new_state;
}

// Advanced event scheduling for Maybenot defense simulation.
//
// This function implements the core scheduling algorithm that coordinates:
// 1. Network packet events from the simulation queue
// 2. Defense machine scheduled actions (padding/blocking)
// 3. Defense machine internal timers
// 4. Blocking period expiry events
pub fn pick_next_maybenot(
    si: &SimInfo,
    sq: &mut SimQueue,
    topology: &NetworkTopology,
    current_time: Instant,
) -> Option<SimEvent> {
    let client_maybenot = topology.get_maybenot_client();
    let relay_maybenot = topology.get_maybenot_server();

    // Collect scheduled actions and internal timers from Maybenot nodes
    let mut min_scheduled_action = Duration::MAX;
    let mut action_node = client_maybenot;
    let mut min_internal_timer = Duration::MAX;
    let mut timer_node = client_maybenot;

    // Check client Maybenot node
    let state = client_maybenot.get_sim_state().borrow();

    // Check scheduled actions
    for action in state.scheduled_action.iter().flatten() {
        if action.time >= current_time {
            let duration = action.time.duration_since(current_time);
            if duration < min_scheduled_action {
                min_scheduled_action = duration;
            }
        }
    }

    // Check internal timers
    for timer in state.scheduled_internal_timer.iter().flatten() {
        if *timer >= current_time {
            let duration = timer.duration_since(current_time);
            if duration < min_internal_timer {
                min_internal_timer = duration;
            }
        }
    }
    let client_blocking_until = state.blocking_until;
    drop(state);

    // Check server Maybenot node
    let state = relay_maybenot.get_sim_state().borrow();

    // Check scheduled actions
    for action in state.scheduled_action.iter().flatten() {
        if action.time >= current_time {
            let duration = action.time.duration_since(current_time);
            if duration < min_scheduled_action {
                min_scheduled_action = duration;
                action_node = relay_maybenot;
            }
        }
    }

    // Check internal timers
    for timer in state.scheduled_internal_timer.iter().flatten() {
        if *timer >= current_time {
            let duration = timer.duration_since(current_time);
            if duration < min_internal_timer {
                min_internal_timer = duration;
                timer_node = relay_maybenot;
            }
        }
    }
    let server_blocking_until = state.blocking_until;
    drop(state);

    // Check blocking expiry
    let (min_blocking, blocking_is_client) = match (client_blocking_until, server_blocking_until) {
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
    };

    // Check queue
    let queue_next = sq.peek();
    let queue_duration = match queue_next {
        Some(event) => event.time.duration_since(current_time),
        None => Duration::MAX,
    };

    // Debug output
    if min_scheduled_action == Duration::MAX {
        debug!("\tpick_next(): peek_scheduled_action = None");
    } else {
        debug!(
            "\tpick_next(): peek_scheduled_action = {:?}",
            min_scheduled_action
        );
    }

    if min_internal_timer == Duration::MAX {
        debug!("\tpick_next(): peek_scheduled_internal_timer = None");
    } else {
        debug!(
            "\tpick_next(): peek_scheduled_internal_timer = {:?}",
            min_internal_timer
        );
    }

    if min_blocking == Duration::MAX {
        debug!("\tpick_next(): peek_blocked_exp = None");
    } else {
        debug!("\tpick_next(): peek_blocked_exp = {:?}", min_blocking);
    }

    if queue_duration == Duration::MAX {
        debug!("\tpick_next(): peek_queue = None");
    } else {
        debug!(
            "\tpick_next(): peek_queue = {}",
            queue_next.unwrap().display_relative(si)
        );
    }

    // No next event?
    if min_scheduled_action == Duration::MAX
        && min_internal_timer == Duration::MAX
        && min_blocking == Duration::MAX
        && queue_duration == Duration::MAX
    {
        return None;
    }

    // Pick the earliest event

    // Blocking expiry is earliest
    if min_blocking <= min_scheduled_action
        && min_blocking <= min_internal_timer
        && min_blocking <= queue_duration
    {
        debug!("\tpick_next(): picked blocking");

        // Clear blocking state from the appropriate node
        if blocking_is_client {
            client_maybenot.get_sim_state().borrow_mut().blocking_until = None;
        } else {
            relay_maybenot.get_sim_state().borrow_mut().blocking_until = None;
        }

        let e = SimEvent {
            event: TriggerEvent::BlockingEnd,
            time: current_time + min_blocking,
            packet_id: usize::MAX,
            node_id: if blocking_is_client {
                topology.mb_client
            } else {
                topology.mb_server
            },
            link_id: if blocking_is_client {
                topology.nodes[topology.mb_client].get_coreside_out_id()
            } else {
                topology.nodes[topology.mb_server].get_edgeside_out_id()
            },
            bypass: false,
            replace: false,
            contains_padding: false,
            q_sequence_nr: 0,
            #[cfg(debug_assertions)]
            debug_note: None,
        };
        return Some(e);
    }

    // Queue is next
    if queue_duration <= min_scheduled_action && queue_duration <= min_internal_timer {
        debug!("\tpick_next(): picked queue");
        return sq.pop();
    }

    // Internal timer is next
    if min_internal_timer <= min_scheduled_action {
        debug!("\tpick_next(): picked internal timer");
        let target_time = current_time + min_internal_timer;

        if let Some(event) = timer_node.do_internal_timer(target_time) {
            return Some(event);
        }
    }

    // Scheduled action is last
    debug!("\tpick_next(): picked scheduled action");
    let target_time = current_time + min_scheduled_action;

    if let Some(event) = action_node.do_scheduled_action(target_time) {
        return Some(event);
    }
    None
}

// Generic helper functions for Maybenot operations
pub fn maybenot_trigger_update<T: MaybenotNode>(
    node: &T,
    s_event: &SimEvent,
    current_time: &Instant,
    sq: &mut SimQueue,
    _topology: &NetworkTopology,
) {
    let node_id = node.node_id();
    let link_id = node.get_action_link_id();

    // Clone the actions to avoid borrowing issues
    let actions: Vec<_> = {
        let mut state = node.get_sim_state().borrow_mut();
        state
            .framework
            .trigger_events(std::slice::from_ref(&s_event.event), *current_time)
            .cloned()
            .collect()
    };

    // Now process actions with a fresh borrow
    for action in actions {
        let mut state = node.get_sim_state().borrow_mut();
        let trigger_delay = state.trigger_delay();
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
                    time: *current_time + timeout + trigger_delay,
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
                    time: *current_time + timeout + trigger_delay,
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
                    sq.push(SimEvent {
                        event: TriggerEvent::TimerBegin { machine },
                        time: *current_time,
                        packet_id: usize::MAX,
                        node_id,
                        link_id,
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

pub fn maybenot_do_internal_timer<T: MaybenotNode>(node: &T, target: Instant) -> Option<SimEvent> {
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

    machine.map(|machine| SimEvent {
        event: TriggerEvent::TimerEnd { machine },
        time: target,
        packet_id: usize::MAX,
        node_id: node.node_id(),
        link_id: node.get_action_link_id(),
        bypass: false,
        replace: false,
        contains_padding: false,
        q_sequence_nr: 0,
        #[cfg(debug_assertions)]
        debug_note: None,
    })
}

pub fn maybenot_do_scheduled_action<T: MaybenotNode>(
    node: &T,
    target: Instant,
) -> Option<SimEvent> {
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
        } => Some(SimEvent {
            event: TriggerEvent::PaddingSent { machine },
            time: a.time + state.action_delay(),
            packet_id: usize::MAX,
            node_id: node.node_id(),
            link_id: node.get_action_link_id(),
            bypass,
            replace,
            contains_padding: true,
            q_sequence_nr: 0,
            #[cfg(debug_assertions)]
            debug_note: None,
        }),
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

            Some(SimEvent {
                event: TriggerEvent::BlockingBegin { machine },
                time: a.time + state.action_delay() + state.reporting_delay(),
                packet_id: usize::MAX,
                node_id: node.node_id(),
                link_id: node.get_action_link_id(),
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
