use std::{cmp::max, sync::Arc, time::Duration};

use crate::linktrace::LinkTrace;

/////// High-performance enum-based link dispatch
#[derive(Debug, Clone)]
pub enum LinkType {
    FixedTput(FixedTputLink),
    HiTraceTput(HiTraceTputLink),
    StdTraceTput(StdTraceTputLink),
}

impl LinkType {
    pub fn sample(&mut self, current_duration: Duration) -> Duration {
        match self {
            LinkType::FixedTput(link) => link.sample(current_duration),
            LinkType::HiTraceTput(link) => link.sample(current_duration),
            LinkType::StdTraceTput(link) => link.sample(current_duration),
        }
    }

    pub fn link_id(&self) -> usize {
        match self {
            LinkType::FixedTput(link) => link.id,
            LinkType::HiTraceTput(link) => link.id,
            LinkType::StdTraceTput(link) => link.id,
        }
    }

    pub fn from_node(&self) -> usize {
        match self {
            LinkType::FixedTput(link) => link.from,
            LinkType::HiTraceTput(link) => link.from,
            LinkType::StdTraceTput(link) => link.from,
        }
    }

    pub fn to_node(&self) -> usize {
        match self {
            LinkType::FixedTput(link) => link.to,
            LinkType::HiTraceTput(link) => link.to,
            LinkType::StdTraceTput(link) => link.to,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            LinkType::FixedTput(_) => "FixedTput",
            LinkType::HiTraceTput(_) => "HiTraceTput",
            LinkType::StdTraceTput(_) => "StdTraceTput",
        }
    }

    pub fn get_prop_us_fixed(&self) -> Duration {
        match self {
            LinkType::FixedTput(link) => link.prop_us,
            LinkType::HiTraceTput(link) => link.prop_us,
            LinkType::StdTraceTput(link) => link.prop_us,
        }
    }

    pub fn get_prop_us_variable(&self, current_time_ms: usize) -> Duration {
        let prop_us_vec = match self {
            LinkType::FixedTput(link) => &link.prop_us_vec,
            LinkType::HiTraceTput(link) => &link.prop_us_vec,
            LinkType::StdTraceTput(link) => &link.prop_us_vec,
        };

        // Use time-dependent propagation with bounds checking
        let index = if current_time_ms >= prop_us_vec.len() {
            // If beyond the end of the vector, use the last available value
            prop_us_vec.len() - 1
        } else {
            current_time_ms
        };
        Duration::from_micros(prop_us_vec[index])
    }

    pub fn fixed_propagation(&self) -> bool {
        match self {
            LinkType::FixedTput(link) => link.fixed_propagation,
            LinkType::HiTraceTput(link) => link.fixed_propagation,
            LinkType::StdTraceTput(link) => link.fixed_propagation,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FixedTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub prop_us: Duration,
    pub tput_bps: u64,
    pub next_busy_to_duration: Duration,
    pub fixed_propagation: bool,
    pub prop_us_vec: Vec<u64>,
}

impl FixedTputLink {
    pub fn new(
        id: usize,
        from: usize,
        to: usize,
        prop_us: Duration,
        tput_bps: u64,
        fixed_propagation: bool,
        prop_us_vec: Vec<u64>,
    ) -> Self {
        Self {
            id,
            from,
            to,
            prop_us,
            tput_bps,
            next_busy_to_duration: Duration::default(),
            fixed_propagation,
            prop_us_vec,
        }
    }

    pub fn sample(&mut self, current_duration: Duration) -> Duration {
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        // Calculate the transmission delay for a packet with a given size:
        // this_packet_duration (ns) = (pkt_size * 8 * 1e9) / throughput (bits/s)
        let packet_size_bits = pkt_size * 8;
        let this_packet_duration =
            Duration::from_nanos((packet_size_bits as u64 * 1_000_000_000) / self.tput_bps);

        // Compute the new busy time and any queueing delay.
        let (new_busy_to_dur, queueing_delay_duration) =
            if self.next_busy_to_duration <= current_duration {
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
}

#[derive(Debug, Clone)]
pub struct HiTraceTputLink {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub prop_us: Duration,
    // High resolution sampling state (simplex only)
    next_busy_to: usize,
    linktrace: Arc<LinkTrace>,
    pub fixed_propagation: bool,
    pub prop_us_vec: Vec<u64>,
}

impl HiTraceTputLink {
    pub fn new(
        id: usize,
        from: usize,
        to: usize,
        prop_us: Duration,
        linktrace: Arc<LinkTrace>,
        fixed_propagation: bool,
        prop_us_vec: Vec<u64>,
    ) -> Self {
        Self {
            id,
            from,
            to,
            prop_us,
            next_busy_to: 0,
            linktrace,
            fixed_propagation,
            prop_us_vec,
        }
    }

    pub fn sample(&mut self, current_duration: Duration) -> Duration {
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        // Determine the current time slot.
        let current_time_slot = current_duration.as_micros() as usize;

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
    pub prop_us: Duration,
    // Standard resolution sampling state (simplex only)
    next_busy_to: usize,
    busy_ns_in_slot: u64,
    bw_trace: Vec<i32>,
    pub fixed_propagation: bool,
    pub prop_us_vec: Vec<u64>,
}

impl StdTraceTputLink {
    pub fn new(
        id: usize,
        from: usize,
        to: usize,
        prop_us: Duration,
        linktrace: Arc<LinkTrace>,
        fixed_propagation: bool,
        prop_us_vec: Vec<u64>,
    ) -> Self {
        // For simplex operation, use the single trace
        let bw_trace = linktrace.bw_trace.clone();

        Self {
            id,
            from,
            to,
            prop_us,
            next_busy_to: 0,
            busy_ns_in_slot: 0,
            bw_trace,
            fixed_propagation,
            prop_us_vec,
        }
    }

    pub fn sample(&mut self, current_duration: Duration) -> Duration {
        // pkt_size should come as call parameter, is hardwired for now
        let pkt_size = 1500;

        // Determine the current time slot.
        let current_time_slot = current_duration.as_millis() as usize;
        let current_slot_ns_position: u64 = (current_duration.as_nanos() % 1_000_000) as u64;

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
        let ns_to_send_remaining = ((remaining_pkt_size as f64 / self.bw_trace[slot_index] as f64)
            * 1e6_f64)
            .round() as u64;
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
