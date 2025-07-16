//! The main queue of events in the simulator.

use std::{
    time::{Duration, Instant},
    collections::HashMap,};

use maybenot::event::TriggerEvent;

use crate::{
    event_to_usize,
    queue_event::{EventQueue, Queue},
    SimEvent, EventKind
};

/// SimQueue represents the queue of events that are to be processed by the
/// simulator. It is a wrapper around an EventQueue for the client, server and
/// webserver. The goal is to never have to search through
/// any of the queues, but to be able to directly access the next event
/// that is to be processed with as little work as possible.
#[derive(Debug, Clone)]
pub struct SimQueue {
    pub(crate) client: EventQueue,
    pub(crate) server: EventQueue,
    pub(crate) webserver: EventQueue,
    // The maximum number of packets/cells (depends on trace) per second before
    // adding delay due to a simulated bottleneck. None means no limit.
    pub(crate) max_pps: Option<usize>,
    // For now store all of the traffic trace data in the queue, although being overkill.
    pub(crate) dependent_tx: HashMap<usize, Vec<(usize, i64, EventKind)>>,
}

impl Default for SimQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl SimQueue {
    pub fn new() -> SimQueue {
        SimQueue {
            client: EventQueue::new(),
            server: EventQueue::new(),
            webserver: EventQueue::new(),
            max_pps: None,
            dependent_tx: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.client.len() + self.server.len() + self.webserver.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn no_normal_packets(&self) -> bool {
        self.client.no_normal_packets()
            && self.server.no_normal_packets()
            && self.webserver.no_normal_packets()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        event: TriggerEvent,
        packet_idx: usize,
        contains_padding: bool,
        time: Instant,
        delay: Duration,
    ) {
        self.push_sim(SimEvent {
            event,
            time,
            packet_idx,
            contains_padding,
            bypass: false,
            replace: false,
            #[cfg(debug_assertions)]
            debug_note: None,
        });
    }

    pub fn push_sim(&mut self, item: SimEvent) {
        self.webserver.push(item);
    }

    pub fn peek(&self, current_time: Instant) -> (Option<&SimEvent>, Queue, Duration) {
        if self.is_empty() {
            return (None, Queue::Blocking, Duration::default());
        }

        // Get the peek values from each side.
        let (client, client_queue, client_duration) = self.client.peek(current_time);
        let (server, server_queue, server_duration) = self.server.peek(current_time);
        let (webserver, webserver_queue, webserver_duration) = self.webserver.peek(current_time);

        // Collect only those events that are present.
        let mut events: Vec<(&SimEvent, Queue, Duration)> = Vec::new();
        if let Some(ev) = client {
            events.push((ev, client_queue, client_duration));
        }
        if let Some(ev) = server {
            events.push((ev, server_queue, server_duration));
        }
        if let Some(ev) = webserver {
            events.push((ev, webserver_queue, webserver_duration));
        }

        // If no events are present, return default.
        if events.is_empty() {
            return (None, Queue::Blocking, Duration::default());
        }

        // Choose the event with the smallest duration, and if equal, then based on event type.
        // The ordering function uses `cmp` on durations, and if equal, compares the event values.
        let best = events
            .into_iter()
            .min_by(|a, b| {
                a.2.cmp(&b.2)
                    .then_with(|| event_to_usize(&a.0.event).cmp(&event_to_usize(&b.0.event)))
            })
            .unwrap();

        (Some(best.0), best.1, best.2)
    }

    pub fn pop(&mut self, q: Queue, is_client: bool, is_webserver: bool) -> Option<SimEvent> {
        match is_client {
            true => self.client.pop(q),
            false => match is_webserver {
                true => self.webserver.pop(q),
                false => self.server.pop(q),
            },
        }
    }

    pub fn peek_blocking(
        &self,
        active_blocking_bypassable: bool,
        is_client: bool,
        is_webserver: bool,
    ) -> (Option<&SimEvent>, Queue) {
        if is_client {
            peek_blocking(&self.client, active_blocking_bypassable)
        } else if is_webserver {
            peek_blocking(&self.webserver, active_blocking_bypassable)
        } else {
            peek_blocking(&self.server, active_blocking_bypassable)
        }
    }
    

    pub fn pop_blocking(
        &mut self,
        q: Queue,
        bypassable: bool,
        is_client: bool,
        is_webserver: bool,
    ) -> Option<SimEvent> {
        if bypassable {
            match is_client {
                true => self.client.blocking.pop(),                
                false => match is_webserver {
                    true => self.webserver.blocking.pop(),
                    false => self.server.blocking.pop(),
                }
            }
        } else {
            self.pop(q, is_client, is_webserver)
        }
    }

    pub fn peek_non_blocking(
        &self,
        bypassable: bool,
        is_client: bool,
        is_webserver: bool,
    ) -> (Option<&SimEvent>, Queue) {
        match is_client {
            true => peek_non_blocking(&self.client, bypassable),
            false => match is_webserver {
                true => peek_non_blocking(&self.webserver, bypassable),
                false => peek_non_blocking(&self.server, bypassable),
            },
        }
    }

    pub fn get_first_time(&self) -> Option<Instant> {
        let c = self.client.get_first_base_time();
        let s = self.server.get_first_base_time();
        let w = self.webserver.get_first_base_time();

        [c, s, w].into_iter().flatten().min()
    }
}

fn peek_blocking(
    queue: &EventQueue,
    active_blocking_bypassable: bool,
) -> (Option<&SimEvent>, Queue) {
    if active_blocking_bypassable {
        // only blocking events are then blocking
        (queue.peek_blocking(), Queue::Blocking)
    } else {
        // if the current blocking is not bypassable, then we need to
        // consider bypassable events as also blocking
        let b = queue.peek_blocking();
        let bb = queue.peek_bypassable();

        if b > bb {
            (b, Queue::Blocking)
        } else {
            (bb, Queue::Bypassable)
        }
    }
}

fn peek_non_blocking(queue: &EventQueue, bypassable: bool) -> (Option<&SimEvent>, Queue) {
    if bypassable {
        // if the current blocking is bypassable, then we need to consider
        // bypassable as non-blocking
        let bb = queue.peek_bypassable();
        let (n, nq) = queue.peek_non_blocking();

        if bb > n {
            (bb, Queue::Bypassable)
        } else {
            (n, nq)
        }
    } else {
        queue.peek_non_blocking()
    }
}
