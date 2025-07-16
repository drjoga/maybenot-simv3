use std::{
    cmp::max,
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};


use crate::{
    linktrace::{mk_start_instant, LinkTrace, load_linktrace_from_file},
};


/////// High-performance enum-based link dispatch
#[derive(Debug, Clone)]
pub enum LinkType {
    BottleneckTput(BottleneckTputLink),
    FixedTput(FixedTputLink),
    HiTraceTput(HiTraceTputLink),
    StdTraceTput(StdTraceTputLink),
}


impl LinkType {
    pub fn sample(&mut self, current_duration: Duration) -> Duration {
        match self {
            LinkType::BottleneckTput(link) => link.sample(current_duration),
            LinkType::FixedTput(link) => link.sample(current_duration),
            LinkType::HiTraceTput(link) => link.sample(current_duration),
            LinkType::StdTraceTput(link) => link.sample(current_duration),
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

    pub fn prop_ms(&self) -> Duration {
        match self {
            LinkType::BottleneckTput(link) => link.prop_ms,
            LinkType::FixedTput(link) => link.prop_ms,
            LinkType::HiTraceTput(link) => link.prop_ms,
            LinkType::StdTraceTput(link) => link.prop_ms,
        }
    }
}




#[derive(Debug, Clone)]
pub struct BottleneckTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub prop_ms: Duration,
    network_bottleneck: NetworkBottleneck,
}

impl BottleneckTputLink {
    pub fn new(id: usize, from: usize, to: usize, prop_ms: Duration, window: Duration, queue_pps: Option<usize>) -> Self {
        Self {
            id,
            from,
            to,
            prop_ms,
            network_bottleneck: NetworkBottleneck::new(window, queue_pps),
        }
    }
    pub fn sample(&self, _current_duration: Duration,) -> Duration {
        // Simplified for immutable access - returns a basic transmission delay
        Duration::from_millis(10)
    }
}

#[derive(Debug, Clone)]
pub struct FixedTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub prop_ms: Duration,
    pub tput_bps: u64,
    pub next_busy_to_duration: Duration,
}

impl FixedTputLink {
    pub fn new(id: usize, from: usize, to: usize, prop_ms: Duration, tput_bps: u64) -> Self {
        Self {
            id,
            from,
            to,
            prop_ms,
            tput_bps,
            next_busy_to_duration: Duration::default(),

        }
    }

    pub fn sample(
        &mut self,
        current_duration: Duration,
    ) -> Duration {
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;


        // Calculate the transmission delay for a packet with a given size:
        // this_packet_duration (ns) = (pkt_size * 8 * 1e9) / throughput (bits/s)
        let packet_size_bits = pkt_size * 8;
        let this_packet_duration =
            Duration::from_nanos((packet_size_bits as u64 * 1_000_000_000) / self.tput_bps);

        // Compute the new busy time and any queueing delay.
        let (new_busy_to_dur, queueing_delay_duration) = if self.next_busy_to_duration <= current_duration
        {
            // No waiting required.
            (current_duration + this_packet_duration, Duration::default())
        } else {
            // Packet must wait: the queueing delay is the gap between current time and the stored busy time.
            let q_delay = self.next_busy_to_duration - current_duration;
            (self.next_busy_to_duration + this_packet_duration, q_delay)
        };

        // Update the stored busy time (in ns) from the computed Duration.
        self.next_busy_to_duration = new_busy_to_dur;

        queueing_delay_duration + this_packet_duration
    }


    pub fn sample2(&self, _current_duration: Duration) -> Duration {
        // Simplified for immutable access - returns a basic transmission delay
        Duration::from_millis(0)
    }
}

#[derive(Debug, Clone)]
pub struct HiTraceTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub prop_ms: Duration,
    // High resolution sampling state (simplex only)
    next_busy_to: usize,
    sim_trace_startinstant: Instant,
    linktrace: Arc<LinkTrace>,
}

impl HiTraceTputLink {
    pub fn new(id: usize, from: usize, to: usize, prop_ms: Duration, linktrace: Arc<LinkTrace>) -> Self {
        Self {
            id,
            from,
            to,
            prop_ms,
            next_busy_to: 0,
            sim_trace_startinstant: mk_start_instant(),
            linktrace,
        }
    }
    
    pub fn sample(&mut self, current_duration: Duration) -> Duration {
        // Convert Duration to Instant for compatibility with existing algorithm
        let current_time = self.sim_trace_startinstant + current_duration;
        
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        // Compute the simulation relative duration and determine the current time slot.
        let sim_relative_duration = current_time.duration_since(self.sim_trace_startinstant);
        let current_time_slot = sim_relative_duration.as_micros() as usize;

        let busy_to;
        let mut queueing_delay_duration = Duration::default();
        let this_packet_duration;

        // Depending on whether the current time slot is after the previous packet finished,
        // choose the lookup parameters and compute durations.
        if self.next_busy_to <= current_time_slot {
            // For simplex operation, use the single trace
            busy_to = self.linktrace.get_busy_to(current_time_slot, pkt_size);
            this_packet_duration = Duration::from_micros((busy_to - current_time_slot) as u64);
        } else {
            // For simplex operation, use the single trace
            busy_to = self.linktrace.get_busy_to(self.next_busy_to, pkt_size);
            queueing_delay_duration =
                Duration::from_micros((self.next_busy_to - current_time_slot) as u64);
            this_packet_duration = Duration::from_micros((busy_to - self.next_busy_to) as u64);
        }

        // Make sure that we are not at the end of the link trace
        assert_ne!(
            busy_to, 0,
            "Packet to be scheduled outside of link trace end"
        );

        // Update next_busy_to in preparation for the next packet
        self.next_busy_to = busy_to;

        // Return only the total delay (queueing + transmission)
        if queueing_delay_duration > Duration::default() {
            queueing_delay_duration + this_packet_duration
        } else {
            this_packet_duration
        }
    }
    
    pub fn reset(&mut self) {
        self.next_busy_to = 0;
    }
}

#[derive(Debug, Clone)]
pub struct StdTraceTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub prop_ms: Duration,
    // Standard resolution sampling state (simplex only)
    next_busy_to: usize,
    busy_ns_in_slot: u64,
    sim_trace_startinstant: Instant,
    bw_trace: Vec<i32>,
}

impl StdTraceTputLink {
    pub fn new(id: usize, from: usize, to: usize, prop_ms: Duration, linktrace: Arc<LinkTrace>) -> Self {
        // For simplex operation, use the single trace
        let bw_trace = linktrace.bw_trace.clone();
        
        Self {
            id,
            from,
            to,
            prop_ms,
            next_busy_to: 0,
            busy_ns_in_slot: 0,
            sim_trace_startinstant: mk_start_instant(),
            bw_trace,
        }
    }
    
    pub fn sample(&mut self, current_duration: Duration) -> Duration {
        // Convert Duration to Instant for compatibility with existing algorithm
        let current_time = self.sim_trace_startinstant + current_duration;
        
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        // Compute the simulation relative duration and determine the current time slot.
        let sim_relative_duration = current_time.duration_since(self.sim_trace_startinstant);
        let current_time_slot = sim_relative_duration.as_millis() as usize;
        let current_slot_ns_position: u64 = (sim_relative_duration.as_nanos() % 1_000_000) as u64;

        // Note: Timing calculation code below is intricate, order between statements can matter.
        // Establish if the packet will have to queue, or can start sending immediately
        let packet_sees_queuing = self.next_busy_to > current_time_slot
            || ((self.next_busy_to == current_time_slot)
                && (self.busy_ns_in_slot > current_slot_ns_position));

        // If we are in a new slot after network having been idle, reset busy_ns_in_slot
        if self.next_busy_to < current_time_slot {
            self.busy_ns_in_slot = 0
        };

        // Get the slot index for the slot where we can first send
        let mut slot_index = max(current_time_slot, self.next_busy_to);

        // Get the ns offset inside the slot we first can send in
        let first_slot_start_send_ns = if slot_index == current_time_slot {
            max(current_slot_ns_position, self.busy_ns_in_slot)
        } else {
            self.busy_ns_in_slot
        };

        let mut ns_to_slot_end = 1_000_000 - first_slot_start_send_ns;
        let mut bytes_to_slot_end = (ns_to_slot_end * self.bw_trace[slot_index] as u64) / 1_000_000;

        // Packet transmission take place possibly across multiple slots
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
                slot_index < self.bw_trace.len(),
                "Packet to be scheduled outside of link trace end: slot_index {} >= bw_trace.len() {}",
                slot_index,
                self.bw_trace.len()
            );
            bytes_to_slot_end = self.bw_trace[slot_index] as u64;
            ns_to_slot_end = 1_000_000;
        }

        // We are now at the slot which allows the last byte of the packet to be sent
        let ns_to_send_remaining =
            ((remaining_pkt_size as f64 / self.bw_trace[slot_index] as f64) * 1e6_f64).round() as u64;
        this_packet_duration_ns += ns_to_send_remaining;

        // Either we are in the first slot, or we have moved, this affects send_end_ns calculation
        let last_slot_send_end_ns = if slot_boundaries_crossed == 0 {
            first_slot_start_send_ns + ns_to_send_remaining
        } else {
            ns_to_send_remaining
        };

        // Update the struct values for next invocation
        self.next_busy_to = slot_index;
        self.busy_ns_in_slot = last_slot_send_end_ns;

        let total_ns_now_to_end: u64 = ((self.next_busy_to - current_time_slot) as i64 * 1_000_000
            + (last_slot_send_end_ns as i64 - current_slot_ns_position as i64) as i64)
            as u64;

        // Round to us resolution and make duration
        let total_ns_now_to_end = (total_ns_now_to_end / 1000) * 1000;
        let this_packet_duration_ns = (this_packet_duration_ns / 1000) * 1000;

        let total_queueing_delay_duration = Duration::from_nanos(total_ns_now_to_end);
        let this_packet_duration = Duration::from_nanos(this_packet_duration_ns);

        // Return only the total delay (queueing + transmission)
        if packet_sees_queuing {
            total_queueing_delay_duration
        } else {
            this_packet_duration
        }
    }
    
    pub fn reset(&mut self) {
        self.next_busy_to = 0;
        self.busy_ns_in_slot = 0;
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
    // Parse prop_ms parameter (required for all link types)
    let prop_ms = params
        .get("prop_ms")
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(0)); // Default to 0ms if not specified

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
            
            Ok(LinkType::BottleneckTput(BottleneckTputLink::new(id, from, to, prop_ms, window, queue_pps)))
        }
        "FixedTput" => {
            // Simplex link - requires tput_bps parameter
            let tput = params
                .get("tput_bps")
                .ok_or("FixedTput requires tput_bps parameter")?
                .parse::<u64>()
                .map_err(|_| "Invalid tput_bps value - must be a valid u64")?;
            
            Ok(LinkType::FixedTput(FixedTputLink::new(id, from, to, prop_ms, tput)))
        }
        "HiTraceTput" => {
            let trace_file = params
                .get("trace_file")
                .ok_or("HiTraceTput requires trace_file parameter")?;
            
            let linktrace = load_linktrace_from_file(trace_file)
                .map_err(|e| format!("Failed to load trace file '{}': {}", trace_file, e))?;
            
            Ok(LinkType::HiTraceTput(HiTraceTputLink::new(id, from, to, prop_ms, linktrace)))
        }
        "StdTraceTput" => {
            let trace_file = params
                .get("trace_file")
                .ok_or("StdTraceTput requires trace_file parameter")?;
            
            let linktrace = load_linktrace_from_file(trace_file)
                .map_err(|e| format!("Failed to load trace file '{}': {}", trace_file, e))?;
            
            Ok(LinkType::StdTraceTput(StdTraceTputLink::new(id, from, to, prop_ms, linktrace)))
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
}

#[derive(Debug, Clone)]
pub enum ExtendedNetwork {
    Bottleneck(NetworkBottleneck),
}

impl ExtendedNetwork {
    pub fn new_bottleneck(window: Duration, queue_pps: Option<usize>) -> Self {
        ExtendedNetwork::Bottleneck(NetworkBottleneck::new(window, queue_pps))
    }


    pub fn sample(
        &mut self,
        current_time: &Instant,
        is_client: bool,
    ) -> (Duration, Option<Duration>) {
        match self {
            ExtendedNetwork::Bottleneck(bn) => bn.sample(current_time, is_client),
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



