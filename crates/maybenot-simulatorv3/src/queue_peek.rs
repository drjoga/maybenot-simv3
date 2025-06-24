//! Functions for peeking at the possible next events in the simulation.

use std::time::{Duration, Instant};

use log::debug;
use maybenot::{event::Event, Machine};

use crate::{queue::SimQueue, queue_event::Queue, RngSource, ScheduledAction, SimState};

pub(crate) fn peek_queue<M: AsRef<[Machine]>>(
    sq: &SimQueue,
    client: &SimState<M, RngSource>,
    server: &SimState<M, RngSource>,
    webserver: &SimState<M, RngSource>,
    earliest: Duration,
    current_time: Instant,
) -> (Duration, Queue) {
    // easy: no queue to consider
    if sq.is_empty() {
        return (Duration::MAX, Queue::Blocking);
    }

    // peek, computing the duration since the current time for the peeked event
    let (peek, queue, duration_since) = sq.peek(current_time);
    let peek = peek.unwrap();

    // if the earliest peeked is *after* the earliest found by other peeks(),
    // then looking further is pointless as far as pick_next() is concerned
    if duration_since > earliest {
        return (Duration::MAX, Queue::Blocking);
    }

    // easy: non-blocking event first
    if !peek.event.is_event(Event::TunnelSent) {
        return (duration_since, queue, );
    }

    let client_blocking = client.blocking_until.is_some();
    let server_blocking = server.blocking_until.is_some();
    let webserver_blocking = webserver.blocking_until.is_some();

    // easy: no active blocking to consider
    if !client_blocking && !server_blocking && !webserver_blocking {
        return (duration_since, queue);
    }


    // not lucky, things get ugly...we have to consider all sides: find
    // earliest from client, server, and webserver sides.
    let (c_d, c_q, c_b, c_wb) = peek_queue_earliest_side(
        sq,
        client.blocking_until,
        client.blocking_bypassable,
        current_time,
        true,
        false,
    );
    let (s_d, s_q, s_b, s_wb) = peek_queue_earliest_side(
        sq,
        server.blocking_until,
        server.blocking_bypassable,
        current_time,
        false,
        false,
    );
    let (w_d, w_q, w_b, w_wb) = peek_queue_earliest_side(
        sq,
        server.blocking_until,
        server.blocking_bypassable,
        current_time,
        false,
        true,
    );

    // Debug message includes values from all three sides.
    debug!(
        "peek_queue: c_d={:?}, c_q={:?}, c_b={}, c_wb={:?}, s_d={:?}, s_q={:?}, s_b={}, s_wb={:?}, w_d={:?}, w_q={:?}, w_b={}, w_wb={:?}",
        c_d, c_q, c_b, c_wb,
        s_d, s_q, s_b, s_wb,
        w_d, w_q, w_b, w_wb
    );

    // Pick the earliest among client, server, and webserver.
    if c_d <= s_d && c_d <= w_d {
        (c_d, c_q)
    } else if s_d <= w_d {
        (s_d, s_q)
    } else {
        (w_d, w_q)
    }
}

// Here be dragons: surprisingly annoying function to get right and fast.
// Closely tied to how SimQueue is implemented.
fn peek_queue_earliest_side(
    sq: &SimQueue,
    blocking_until: Option<Instant>,
    blocking_bypassable: bool,
    current_time: Instant,
    is_client: bool,
    is_webserver: bool,
) -> (Duration, Queue, bool, bool) {
    debug!("peek_queue_earliest_side: is_client={}", is_client);
    debug!("peek_queue_earliest_side: is_webserver={}", is_webserver);
    // OK, bummer, we have to peek for the next blocking and non-blocking: note
    // that this takes into account if blocking is bypassable or not, picking
    // the earliest next event from the queue.
    let (peek_blocking, blocking_queue) = sq.peek_blocking(blocking_bypassable, is_client, is_webserver);
    let (peek_non_blocking, non_blocking_queue) =
        sq.peek_non_blocking(blocking_bypassable, is_client, is_webserver);

    // easy: no events to consider
    if peek_blocking.is_none() && peek_non_blocking.is_none() {
        return (Duration::MAX, Queue::Blocking, is_client, is_webserver);
    }

    // take the blocking_until into account, if no set, use current time as a
    // placeholder
    let blocking_until = blocking_until.unwrap_or(current_time);

    // easy: only one event to consider
    if peek_blocking.is_none() {
        let peek_non_blocking_time = peek_non_blocking.unwrap().time;
        return (
            peek_non_blocking_time.duration_since(current_time),
            non_blocking_queue,
            is_client,
            is_webserver,
        );
    }
    if peek_non_blocking.is_none() {
        return (
            peek_blocking
                .unwrap()
                .time
                .max(blocking_until)
                .duration_since(current_time),
            blocking_queue,
            is_client,
            is_webserver,
        );
    }

    // consider both events, taking blocking into account
    let peek_blocking = peek_blocking.unwrap();
    let peek_non_blocking = peek_non_blocking.unwrap();

    debug!(
        "\tpeek_queue_earliest_side: peek_blocking={:?}, blocking_queue={:?}",
        peek_blocking, blocking_queue
    );
    debug!(
        "\tpeek_queue_earliest_side: peek_non_blocking={:?}, non_blocking_queue={:?}",
        peek_non_blocking, non_blocking_queue
    );

    // take network delay into account for non-blocking events
    let peek_non_blocking_time = peek_non_blocking.time;

    // a bit verbose, but on equal, we want to prioritize the base queue while
    // not prioritizing the internal queue, which are both non-blocking
    let blocking_first = match peek_blocking
        .time
        .max(blocking_until)
        .cmp(&peek_non_blocking_time)
    {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        // blocking only if the queue is not the base queue
        std::cmp::Ordering::Equal => non_blocking_queue != Queue::Base,
    };
    debug!(
        "\tpeek_queue_earliest_side: blocking_first={}",
        blocking_first
    );
    if blocking_first {
        (
            peek_blocking
                .time
                .max(blocking_until)
                .duration_since(current_time),
            blocking_queue,
            is_client,
            is_webserver,
        )
    } else {
        (
            peek_non_blocking_time.duration_since(current_time),
            non_blocking_queue,
            is_client,
            is_webserver,
        )
    }
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
    blocking_w: Option<Instant>,
    current_time: Instant,
) -> (Duration, bool, bool) {
    // We'll track the earliest instant along with flags:
    // (instant, is_c, is_w)
    let earliest = [
        blocking_c.map(|t| (t, true, false)),
        blocking_s.map(|t| (t, false, false)),
        blocking_w.map(|t| (t, false, true)),
    ]
    .into_iter()
    .flatten()
    .min_by_key(|(t, _, _)| *t);

    match earliest {
        Some((t, is_c, is_w)) => (t.duration_since(current_time), is_c, is_w),
        // If none of the blockings are set, return Duration::MAX.
        None => (Duration::MAX, true, false),
    }
}
