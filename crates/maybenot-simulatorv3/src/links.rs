use core::panic;
use std::{
    cmp::max,
    collections::VecDeque,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use log::debug;
use maybenot::{Machine, TriggerEvent};

use crate::{
    linktrace::{mk_start_instant, LinkTrace},
};


/////

// High-performance enum-based link dispatch
#[derive(Debug, Clone)]
pub enum LinkType {
    BottleneckTput(BottleneckTputLink),
    FixedTput(FixedTputLink),
    HiTraceTput(HiTraceTputLink),
    StdTraceTput(StdTraceTputLink),
}


impl LinkType {
    pub fn sample(&mut self, current_time: &Instant) -> Duration {
        match self {
            LinkType::BottleneckTput(link) => link.sample(current_time),
            LinkType::FixedTput(link) => link.sample(current_time),
            LinkType::HiTraceTput(link) => link.sample(current_time),
            LinkType::StdTraceTput(link) => link.sample(current_time),
        }
    }

    pub fn link_id(&self) -> usize {
        match self {
            LinkType::BottleneckTput(link) => link.id,
            LinkType::FixedTput(link) => link.id,
            LinkType::HiTraceTput(link) => link.id,
            LinkType::StdTraceTput(link) => link.id,
        }
    }

    pub fn from_node(&self) -> usize {
        match self {
            LinkType::BottleneckTput(link) => link.from,
            LinkType::FixedTput(link) => link.from,
            LinkType::HiTraceTput(link) => link.from,
            LinkType::StdTraceTput(link) => link.from,
        }
    }

    pub fn to_node(&self) -> usize {
        match self {
            LinkType::BottleneckTput(link) => link.to,
            LinkType::FixedTput(link) => link.to,
            LinkType::HiTraceTput(link) => link.to,
            LinkType::StdTraceTput(link) => link.to,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            LinkType::BottleneckTput(_) => "BottleneckTput",
            LinkType::FixedTput(_) => "FixedTput",
            LinkType::HiTraceTput(_) => "HiTraceTput",
            LinkType::StdTraceTput(_) => "StdTraceTput",
        }
    }
}



use crate::nodes::NodeType;

#[derive(Debug, Clone)]
pub struct BottleneckTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    network_bottleneck: NetworkBottleneck,
}

impl BottleneckTputLink {
    pub fn new(id: usize, from: usize, to: usize, window: Duration, queue_pps: Option<usize>) -> Self {
        Self {
            id,
            from,
            to,
            network_bottleneck: NetworkBottleneck::new(window, queue_pps),
        }
    }
}

impl BottleneckTputLink {
    pub fn sample(&mut self, current_time: &Instant) -> Duration {
        // Assume client for now - in real implementation, this should be determined from context
        let (delay, _) = self.network_bottleneck.sample(current_time, true);
        delay
    }
}

#[derive(Debug, Clone)]
pub struct FixedTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    network_linktrace: NetworkLinktrace,
}

impl FixedTputLink {
    pub fn new(id: usize, from: usize, to: usize, client_tput: u64, server_tput: u64) -> Self {
        Self {
            id,
            from,
            to,
            network_linktrace: NetworkLinktrace::new_fixed(client_tput, server_tput),
        }
    }
}

impl FixedTputLink {
    pub fn sample(&mut self, current_time: &Instant) -> Duration {
        let (delay, _) = self.network_linktrace.sample_fixed(current_time, true);
        delay
    }
}

#[derive(Debug, Clone)]
pub struct HiTraceTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    network_linktrace: NetworkLinktrace,
}

impl HiTraceTputLink {
    pub fn new(id: usize, from: usize, to: usize, linktrace: Arc<LinkTrace>) -> Self {
        Self {
            id,
            from,
            to,
            network_linktrace: NetworkLinktrace::new_linktrace(linktrace),
        }
    }
}

impl HiTraceTputLink {
    pub fn sample(&mut self, current_time: &Instant) -> Duration {
        let (delay, _) = self.network_linktrace.sample_hi(current_time, true);
        delay
    }
}

#[derive(Debug, Clone)]
pub struct StdTraceTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    network_linktrace: NetworkLinktrace,
}

impl StdTraceTputLink {
    pub fn new(id: usize, from: usize, to: usize, linktrace: Arc<LinkTrace>) -> Self {
        Self {
            id,
            from,
            to,
            network_linktrace: NetworkLinktrace::new_linktrace(linktrace),
        }
    }
}

impl StdTraceTputLink {
    pub fn sample(&mut self, current_time: &Instant) -> Duration {
        let (delay, _) = self.network_linktrace.sample_std(current_time, true);
        delay
    }
}

// Factory function for creating links from TOML configuration
pub fn create_link(
    link_type: &str,
    id: usize,
    from: usize,
    to: usize,
    params: &std::collections::HashMap<String, String>,
) -> Result<LinkType, String> {
    match link_type {
        "BottleneckTput" => {
            let window = params
                .get("window_ms")
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(1));
            
            let queue_pps = params
                .get("queue_pps")
                .and_then(|s| s.parse::<usize>().ok());
            
            Ok(LinkType::BottleneckTput(BottleneckTputLink::new(id, from, to, window, queue_pps)))
        }
        "FixedTput" => {
            // Support both single tput_bps (simplex) and separate client/server (duplex)
            let tput = params
                .get("tput_bps")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or_else(|| {
                    // Fall back to separate client/server throughput if tput_bps not found
                    let client_tput = params
                        .get("client_tput_bps")
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(1_000_000);
                    client_tput
                });
            
            let server_tput = params
                .get("server_tput_bps")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(tput); // Use same as client if not specified
            
            Ok(LinkType::FixedTput(FixedTputLink::new(id, from, to, tput, server_tput)))
        }
        "HiTraceTput" => {
            let _trace_file = params
                .get("trace_file")
                .ok_or("HiTraceTput requires trace_file parameter")?;
            
            // For now, create a dummy trace - in real implementation, load from file
            let dummy_trace = LinkTrace::new_std_res("10\n10\n", "10\n10\n");
            let linktrace = Arc::new(dummy_trace);
            
            Ok(LinkType::HiTraceTput(HiTraceTputLink::new(id, from, to, linktrace)))
        }
        "StdTraceTput" => {
            let _trace_file = params
                .get("trace_file")
                .ok_or("StdTraceTput requires trace_file parameter")?;
            
            // For now, create a dummy trace - in real implementation, load from file
            let dummy_trace = LinkTrace::new_std_res("10\n10\n", "10\n10\n");
            let linktrace = Arc::new(dummy_trace);
            
            Ok(LinkType::StdTraceTput(StdTraceTputLink::new(id, from, to, linktrace)))
        }
        _ => Err(format!("Unknown link type: {}", link_type)),
    }
}


///// 





// Labels for the different types of simulated networks there are,
// in terms of how the bottleneck is modeled
#[derive(Debug, Clone)]
pub enum ExtendedNetworkLabels {
    Bottleneck,
    Linktrace,
    FixedTput,
}

#[derive(Debug, Clone)]
pub enum ExtendedNetwork {
    Bottleneck(NetworkBottleneck),
    Linktrace(NetworkLinktrace),
}

impl ExtendedNetwork {
    pub fn new_bottleneck(window: Duration, queue_pps: Option<usize>) -> Self {
        ExtendedNetwork::Bottleneck(NetworkBottleneck::new(window, queue_pps))
    }

    pub fn new_linktrace(linktrace: Arc<LinkTrace>) -> Self {
        ExtendedNetwork::Linktrace(NetworkLinktrace::new_linktrace(linktrace))
    }

    pub fn new_fixedtput(client_tput: u64, server_tput: u64) -> Self {
        assert!(client_tput > 0, "Client throughput need to be > 0 bps.");
        ExtendedNetwork::Linktrace(NetworkLinktrace::new_fixed(
            client_tput,
            server_tput,
        ))
    }

    pub fn sample(
        &mut self,
        current_time: &Instant,
        is_client: bool,
    ) -> (Duration, Option<Duration>) {
        match self {
            ExtendedNetwork::Bottleneck(bn) => bn.sample(current_time, is_client),
            ExtendedNetwork::Linktrace(lt) => lt.sample(current_time, is_client),
        }
    }

}

/// a network bottleneck that adds delay to packets above a certain packets per
/// window limit (default 1s window, so pps), and keeps track of the aggregate
/// delay to add to packets due to the bottleneck or accumulated blocking by
/// machines: used to shift the baseline trace time at both client and server
#[derive(Debug, Clone)]
pub struct NetworkBottleneck {
    // the aggregate delay for the client
    pub client_aggregate_base_delay: Duration,
    // the aggregate delay for the server
    pub server_aggregate_base_delay: Duration,
    // the pending aggregate delays to add to packets due to the bottleneck
    //aggregate_delay_queue: BinaryHeap<PendingAggregateDelay>,
    // window counts for the client and server
    client_window: WindowCount,
    server_window: WindowCount,
    // delay added to packets above the limit
    pps_added_delay: Duration,
    // packets per second limit
    pps_limit: usize,
}

impl NetworkBottleneck {
    pub fn new(window: Duration, queue_pps: Option<usize>) -> Self {
        let pps = queue_pps.unwrap_or(usize::MAX);
        // average delay, based on window and limit
        let added_delay = window / pps as u32;

        Self {
            client_window: WindowCount::new(window),
            server_window: WindowCount::new(window),
            pps_added_delay: added_delay,
            client_aggregate_base_delay: Duration::default(),
            server_aggregate_base_delay: Duration::default(),
            //aggregate_delay_queue: BinaryHeap::new(),
            pps_limit: pps,
        }
    }

    pub fn sample(
        &mut self,
        current_time: &Instant,
        is_client: bool,
    ) -> (Duration, Option<Duration>) {
        let window = if is_client {
            &mut self.client_window
        } else {
            &mut self.server_window
        };

        let count = window.add(current_time);
        let delay = if count > self.pps_limit {
            self.pps_added_delay * (count - self.pps_limit) as u32
        } else {
            Duration::default()
        };
        // Previosuly the propagation delay was added here, was in network.delay
        if delay > Duration::default() {
            (delay, Some(delay))
        } else {
            (Duration::default(), None)
        }
    }

}

#[derive(Debug, Clone)]
pub(crate) struct WindowCount {
    window: Duration,
    timestamps: VecDeque<Instant>,
}

impl WindowCount {
    pub fn new(window: Duration) -> Self {
        WindowCount {
            window,
            timestamps: VecDeque::with_capacity(512),
        }
    }

    pub fn add(&mut self, current_time: &Instant) -> usize {
        // add the current time of the event
        self.timestamps.push_back(*current_time);

        // prune old timestamps
        while let Some(&oldest) = self.timestamps.front() {
            if current_time.duration_since(oldest) > self.window {
                self.timestamps.pop_front();
            } else {
                break;
            }
        }

        // return the number of events in the window
        self.timestamps.len()
    }
}

/// a network that adds delay to packets according to the transmission delay
/// provided by a link trace.  Keeps track of the aggregate
/// delay to add to packets due to the bottleneck or accumulated blocking by
/// machines: used to shift the baseline trace time at both client and relay
#[derive(Debug, Clone)]
pub struct NetworkLinktrace {
    // the aggregate delay for the client
    pub client_aggregate_base_delay: Duration,
    // the aggregate delay for the server
    pub server_aggregate_base_delay: Duration,
    // the pending aggregate delays to add to packets due to the bottleneck
    //aggregate_delay_queue: BinaryHeap<PendingAggregateDelay>,
    // packets per second limit
    //pps_limit: usize,
    pub linktrace: Arc<LinkTrace>,
    // The start instant used by parse_trace for the first event at the server side
    sim_trace_startinstant: Instant,
    // Index to next idle slot in the traces, used for hi and std resolution traces
    client_next_busy_to: usize,
    server_next_busy_to: usize,
    // Remaining ns in current slot, used for std resolution traces
    client_busy_ns_in_slot: u64,
    server_busy_ns_in_slot: u64,
    // Bottleneck throughput for client(ul) and server(dl) in bps. Used for fixedTput
    client_tput: u64,
    server_tput: u64,
    client_next_busy_to_duration: Duration,
    server_next_busy_to_duration: Duration,
}

impl NetworkLinktrace {
    pub fn new_linktrace(linktrace: Arc<LinkTrace>) -> Self {
        Self {
            client_aggregate_base_delay: Duration::default(),
            server_aggregate_base_delay: Duration::default(),
            //aggregate_delay_queue: BinaryHeap::new(),
            //pps_limit: usize::MAX,
            linktrace,
            sim_trace_startinstant: mk_start_instant(),
            client_next_busy_to: 0,
            server_next_busy_to: 0,
            client_busy_ns_in_slot: 0,
            server_busy_ns_in_slot: 0,
            client_tput: 0,
            server_tput: 0,
            client_next_busy_to_duration: Duration::default(),
            server_next_busy_to_duration: Duration::default(),
        }
    }

    pub fn new_fixed(client_tput: u64, server_tput: u64) -> Self {
        //Make new dummy linktrace
        let linktrace = LinkTrace::new_std_res("10\n10\n", "10\n10\n");
        Self {
            client_aggregate_base_delay: Duration::default(),
            server_aggregate_base_delay: Duration::default(),
            //aggregate_delay_queue: BinaryHeap::new(),
            //pps_limit: usize::MAX,
            linktrace: Arc::new(linktrace),
            sim_trace_startinstant: mk_start_instant(),
            client_next_busy_to: 0,
            server_next_busy_to: 0,
            client_busy_ns_in_slot: 0,
            server_busy_ns_in_slot: 0,
            client_tput,
            server_tput,
            client_next_busy_to_duration: Duration::default(),
            server_next_busy_to_duration: Duration::default(),
        }
    }

    pub fn sample(
        &mut self,
        current_time: &Instant,
        _is_client: bool,
    ) -> (Duration, Option<Duration>) {
        if self.client_tput > 0 {
            self.sample_fixed(current_time, _is_client)
        } else if self.linktrace.is_tput_trace_high_res {
            self.sample_hi(current_time, _is_client)
        } else {
            self.sample_std(current_time, _is_client)
        }
    }

    fn sample_hi(
        &mut self,
        current_time: &Instant,
        _is_client: bool,
    ) -> (Duration, Option<Duration>) {
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        // Compute the simulation relative duration and determine the current time slot.
        let sim_relative_duration = current_time.duration_since(self.sim_trace_startinstant);
        let current_time_slot = sim_relative_duration.as_micros() as usize;

        let busy_to;
        let mut queueing_delay_duration = Duration::default();
        let this_packet_duration;

        // Choose the appropriate next_busy_to field based on _is_client.
        let next_busy_to = if _is_client {
            &mut self.client_next_busy_to
        } else {
            &mut self.server_next_busy_to
        };

        // Depending on whether the current time slot is after the previous packet finished,
        // choose the lookup paramters and compute durations.
        if *next_busy_to <= current_time_slot {
            busy_to = if _is_client {
                self.linktrace.get_ul_busy_to(current_time_slot, pkt_size)
            } else {
                self.linktrace.get_dl_busy_to(current_time_slot, pkt_size)
            };
            this_packet_duration = Duration::from_micros((busy_to - current_time_slot) as u64);
        } else {
            busy_to = if _is_client {
                self.linktrace.get_ul_busy_to(*next_busy_to, pkt_size)
            } else {
                self.linktrace.get_dl_busy_to(*next_busy_to, pkt_size)
            };
            queueing_delay_duration =
                Duration::from_micros((*next_busy_to - current_time_slot) as u64);
            this_packet_duration = Duration::from_micros((busy_to - *next_busy_to) as u64);
        }

        // Make sure that we are not at the end of the link trace
        assert_ne!(
            busy_to, 0,
            "Packet to be scheduled outside of link trace end"
        );

        // Update next_busy_to in preparation for the next packet
        *next_busy_to = busy_to;

        // Previosuly the propagation delay was added here, was in network.delay
        if queueing_delay_duration > Duration::default() {
            (
                queueing_delay_duration + this_packet_duration,
                Some(queueing_delay_duration),
            )
        } else {
            (this_packet_duration, None)
        }
    }

    pub fn sample_std(
        &mut self,
        current_time: &Instant,
        _is_client: bool,
    ) -> (Duration, Option<Duration>) {
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        // Compute the simulation relative duration and determine the current time slot.
        let sim_relative_duration = current_time.duration_since(self.sim_trace_startinstant);
        let current_time_slot = sim_relative_duration.as_millis() as usize;
        let current_slot_ns_position: u64 = (sim_relative_duration.as_nanos() % 1_000_000) as u64;

        // Choose the appropriate next_busy_to and bytes _in_slot fields based on _is_client.
        let (next_busy_to, busy_ns_in_slot, bw_trace) = if _is_client {
            (
                &mut self.client_next_busy_to,
                &mut self.client_busy_ns_in_slot,
                &self.linktrace.ul_bw_trace,
            )
        } else {
            (
                &mut self.server_next_busy_to,
                &mut self.server_busy_ns_in_slot,
                &self.linktrace.dl_bw_trace,
            )
        };

        // Note: Timing calulation code below is intricate, order beween statements can matter.
        // Establish if the packet will have to queue, or can start sending immediately
        let packet_sees_queuing = *next_busy_to > current_time_slot
            || ((*next_busy_to == current_time_slot)
                && (*busy_ns_in_slot > current_slot_ns_position));

        // If we are in a new slot after network having been idle, reset busy_ns_in_slot
        if *next_busy_to < current_time_slot {
            *busy_ns_in_slot = 0
        };

        // Get the slot index for the slot where we can first send
        let mut slot_index = max(current_time_slot, *next_busy_to);

        // Get the ns offset inside the slot we first can send in
        let first_slot_start_send_ns = if slot_index == current_time_slot {
            max(current_slot_ns_position, *busy_ns_in_slot)
        } else {
            *busy_ns_in_slot
        };

        let mut ns_to_slot_end = 1_000_000 - first_slot_start_send_ns;
        let mut bytes_to_slot_end = (ns_to_slot_end * bw_trace[slot_index] as u64) / 1_000_000;

        // Packet transmssion take place possibly across multiple slots
        let mut remaining_pkt_size = pkt_size;
        let mut this_packet_duration_ns = 0_u64;
        let mut slot_boundaries_crossed = 0_u64;

        // Cross into new slot(s) until the remaining packet bytes fits in the slot
        while remaining_pkt_size > bytes_to_slot_end {
            this_packet_duration_ns += ns_to_slot_end;
            remaining_pkt_size -= bytes_to_slot_end;
            slot_boundaries_crossed += 1;
            slot_index += 1;
            assert!(
                slot_index < bw_trace.len(),
                "Packet to be scheduled outside of link trace end: slot_index {} >= bw_trace.len() {}",
                slot_index,
                bw_trace.len()
            );
            bytes_to_slot_end = bw_trace[slot_index] as u64;
            ns_to_slot_end = 1_000_000;
        }

        // We are now at the slot which allows the last byte of the packet to be sent
        let ns_to_send_remaining =
            ((remaining_pkt_size as f64 / bw_trace[slot_index] as f64) * 1e6_f64).round() as u64;
        this_packet_duration_ns += ns_to_send_remaining;

        // Either we are in the first slot, or we have moved, this affects send_end_ns calculation
        let last_slot_send_end_ns = if slot_boundaries_crossed == 0 {
            first_slot_start_send_ns + ns_to_send_remaining
        } else {
            ns_to_send_remaining
        };

        // Update the struct values for next invocation
        *next_busy_to = slot_index;
        *busy_ns_in_slot = last_slot_send_end_ns;

        let total_ns_now_to_end: u64 = (((*next_busy_to - current_time_slot) as i64 * 1_000_000)
            + (last_slot_send_end_ns as i64 - current_slot_ns_position as i64) as i64)
            as u64;

        // Round to us resolution and make duration
        let total_ns_now_to_end = (total_ns_now_to_end / 1000) * 1000;
        let this_packet_duration_ns = (this_packet_duration_ns / 1000) * 1000;

        let total_queueing_delay_duration = Duration::from_nanos(total_ns_now_to_end);
        let this_packet_duration = Duration::from_nanos(this_packet_duration_ns);

        // Previosuly the propagation delay was added here, was in network.delay
        if packet_sees_queuing {
            //if total_queueing_delay_duration > this_packet_duration {
            (
                total_queueing_delay_duration,
                Some(total_queueing_delay_duration - this_packet_duration),
            )
        } else {
            (this_packet_duration, None)
        }
    }

    pub fn sample_fixed(
        &mut self,
        current_time: &Instant,
        _is_client: bool,
    ) -> (Duration, Option<Duration>) {
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        let current_duration = current_time.duration_since(self.sim_trace_startinstant);

        // Select the appropriate busy-to field and throughput.
        let (next_busy_duration, throughput) = if _is_client {
            (&mut self.client_next_busy_to_duration, self.client_tput)
        } else {
            (&mut self.server_next_busy_to_duration, self.server_tput)
        };

        // Calculate the transmission delay for a packet with a given size:
        // this_packet_duration (ns) = (pkt_size * 8 * 1e9) / throughput (bits/s)
        let packet_size_bits = pkt_size * 8;
        let this_packet_duration =
            Duration::from_nanos((packet_size_bits as u64 * 1_000_000_000) / throughput);

        // Compute the new busy time and any queueing delay.
        let (new_busy_to_dur, queueing_delay_duration) = if *next_busy_duration <= current_duration
        {
            // No waiting required.
            (current_duration + this_packet_duration, Duration::default())
        } else {
            // Packet must wait: the queueing delay is the gap between current time and the stored busy time.
            let q_delay = *next_busy_duration - current_duration;
            (*next_busy_duration + this_packet_duration, q_delay)
        };

        // Update the stored busy time (in ns) from the computed Duration.
        *next_busy_duration = new_busy_to_dur;

        // Previosuly the propagation delay was added here, was in network.delay
        if queueing_delay_duration > Duration::default() {
            (
                queueing_delay_duration + this_packet_duration,
                Some(queueing_delay_duration),
            )
        } else {
            (this_packet_duration, None)
        }
    }

    pub fn reset_linktrace(&mut self) {
        self.client_aggregate_base_delay = Duration::default();
        self.server_aggregate_base_delay = Duration::default();
        // The two lines below are skewing the benchmark timing comparisons...
        //self.aggregate_delay_queue = BinaryHeap::new();
        //self.sim_trace_startinstant = mk_start_instant();
        self.client_next_busy_to = 0;
        self.server_next_busy_to = 0;
        self.client_busy_ns_in_slot = 0;
        self.server_busy_ns_in_slot = 0;
    }
}










// Legacy Link struct for compatibility - consider removing
pub struct SimpleLink {
    pub from: usize,
    pub to: usize,
    pub delay: Duration,
}

impl SimpleLink {
    pub fn new(from: usize, to: usize, delay: Duration) -> Self {
        Self { from, to, delay }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_link_creation() {
        let link = SimpleLink::new(1, 2, Duration::from_millis(10));
        assert_eq!(link.from, 1);
        assert_eq!(link.to, 2);
        assert_eq!(link.delay, Duration::from_millis(10));
    }
}
