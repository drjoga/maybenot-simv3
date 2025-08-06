use maybenot::TriggerEvent;
use crate::{SimulEvent, SimulQueue};
use crate::network::{NetworkTopology, NetworkLinkstate};
use crate::links::LinkType;
use crate::mbn_nodes::{ClientMBN, RelayMBN, RelayMBNtserver};
use std::time::{Duration, Instant};
use log::debug;

#[derive(Debug, Clone)]
pub enum NodeError {
    InvalidEvent(String),
    ProcessingError(String),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::InvalidEvent(msg) => write!(f, "Invalid event: {}", msg),
            NodeError::ProcessingError(msg) => write!(f, "Processing error: {}", msg),
        }
    }
}

impl std::error::Error for NodeError {}

// High-performance enum-based node dispatch
#[derive(Debug)]
pub enum NodeType {
    ClientBasic(ClientBasic),
    RouterBasic(RouterBasic),
    TrafficServerBasic(TrafficServerBasic),
    ClientMBN(ClientMBN),
    RelayMBN(RelayMBN),
    RelayMBNtserver(RelayMBNtserver),
}

impl NodeType {
    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match self {
            NodeType::ClientBasic(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::RouterBasic(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::TrafficServerBasic(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::ClientMBN(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::RelayMBN(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::RelayMBNtserver(node) => node.handle_event(s_event, topology, linkstate, sq),
        }
    }

    pub fn node_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.node_id(),
            NodeType::RouterBasic(node) => node.node_id(),
            NodeType::TrafficServerBasic(node) => node.node_id(),
            NodeType::ClientMBN(node) => node.node_id(),
            NodeType::RelayMBN(node) => node.node_id(),
            NodeType::RelayMBNtserver(node) => node.node_id(),
        }
    }

    pub fn get_coreside_out_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.get_coreside_out_id(),
            NodeType::RouterBasic(node) => node.get_coreside_out_id(),
            NodeType::TrafficServerBasic(_) => panic!("TrafficServerBasic does not have a coreside link"),
            NodeType::ClientMBN(node) => node.get_coreside_out_id(),
            NodeType::RelayMBN(node) => node.get_coreside_out_id(),
            NodeType::RelayMBNtserver(_) => panic!("RelayMBNtserver does not have a coreside link"),
        }
    }

    pub fn get_edgeside_out_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(_) => panic!("ClientBasic does not have an edgeside link"),
            NodeType::RouterBasic(node) => node.get_edgeside_out_id(),
            NodeType::TrafficServerBasic(node) => node.get_edgeside_out_id(),
            NodeType::ClientMBN(_) => panic!("ClientMBN does not have an edgeside link"),
            NodeType::RelayMBN(node) => node.get_edgeside_out_id(),
            NodeType::RelayMBNtserver(node) => node.get_edgeside_out_id(),
        }
    }

    pub fn get_edgeside_in_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(_) => panic!("ClientBasic does not have an edgeside link"),
            NodeType::RouterBasic(_) => panic!("RouterBasic does not have edgeside_in"),
            NodeType::TrafficServerBasic(_) => panic!("TrafficServerBasic does not have edgeside_in"),
            NodeType::ClientMBN(_) => panic!("ClientMBN does not have an edgeside link"),
            NodeType::RelayMBN(node) => node.get_edgeside_in_id(),
            NodeType::RelayMBNtserver(node) => node.get_edgeside_in_id(),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            NodeType::ClientBasic(_) => "ClientBasic",
            NodeType::RouterBasic(_) => "RouterBasic",
            NodeType::TrafficServerBasic(_) => "TrafficServerBasic",
            NodeType::ClientMBN(_) => "ClientMBN",
            NodeType::RelayMBN(_) => "RelayMBN",
            NodeType::RelayMBNtserver(_) => "RelayMBNtserver",
        }
    }
}

// Factory function for creating nodes from TOML configuration
pub fn create_node(
    node_type: &str,
    id: usize,
    coreside_out: Option<usize>,
    edgeside_in: Option<usize>,
    edgeside_out: Option<usize>,
    params: &std::collections::HashMap<String, String>,
) -> Result<NodeType, String> {
    match node_type {
        "ClientBasic" => {
            let coreside = coreside_out
                .ok_or("ClientBasic requires coreside_out")?;
            Ok(NodeType::ClientBasic(ClientBasic::new(id, coreside)))
        },
        "RouterBasic" => {
            let coreside_out_val = coreside_out
                .ok_or("RouterBasic requires coreside_out")?;
            let edgeside_in_val = edgeside_in
                .ok_or("RouterBasic requires edgeside_in")?;
            let edgeside_out_val = edgeside_out
                .ok_or("RouterBasic requires edgeside_out")?;
            Ok(NodeType::RouterBasic(RouterBasic::new(id, coreside_out_val, edgeside_in_val, edgeside_out_val)))
        },
        "TrafficServerBasic" => {
            let edgeside = edgeside_out
                .ok_or("TrafficServerBasic requires edgeside_out")?;
            Ok(NodeType::TrafficServerBasic(TrafficServerBasic::new(id, edgeside)))
        },
        "ClientMBN" => {
            let coreside = coreside_out
                .ok_or("ClientMBN requires coreside_out")?;
            
            // Parse MBN-specific parameters from params HashMap
            let machines = params
                .get("machines")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_else(Vec::new);
                       
            let max_padding_frac = params
                .get("max_padding_frac")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            
            let max_blocking_frac = params
                .get("max_blocking_frac")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            
            let insecure_rng_seed = params
                .get("insecure_rng_seed")
                .and_then(|s| s.parse::<u64>().ok());
            
            Ok(NodeType::ClientMBN(ClientMBN::new(
                id, coreside, machines, Instant::now(), 
                max_padding_frac, max_blocking_frac, insecure_rng_seed
            )))
        },
        "RelayMBN" => {
            let coreside_out_val = coreside_out
                .ok_or("RelayMBN requires coreside_out")?;
            let edgeside_in_val = edgeside_in
                .ok_or("RelayMBN requires edgeside_in")?;
            let edgeside_out_val = edgeside_out
                .ok_or("RelayMBN requires edgeside_out")?;
            
            // Parse MBN-specific parameters from params HashMap
            let machines = params
                .get("machines")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_else(Vec::new);
            
            let max_padding_frac = params
                .get("max_padding_frac")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            
            let max_blocking_frac = params
                .get("max_blocking_frac")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            
            let insecure_rng_seed = params
                .get("insecure_rng_seed")
                .and_then(|s| s.parse::<u64>().ok());
            
            Ok(NodeType::RelayMBN(RelayMBN::new(
                id, coreside_out_val, edgeside_in_val, edgeside_out_val, machines, Instant::now(),
                max_padding_frac, max_blocking_frac, insecure_rng_seed
            )))
        },
        "RelayMBNtserver" => {
            // RelayMBNtserver uses edgeside_in and edgeside_out
            let edgeside_out_val = edgeside_out
                .ok_or("RelayMBNtserver requires edgeside_out")?;
            let edgeside_in_val = edgeside_in
                .ok_or("RelayMBNtserver requires edgeside_in")?;
            
            // Parse MBN-specific parameters from params HashMap
            let machines = params
                .get("machines")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_else(Vec::new);
            
            let max_padding_frac = params
                .get("max_padding_frac")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            
            let max_blocking_frac = params
                .get("max_blocking_frac")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            
            let insecure_rng_seed = params
                .get("insecure_rng_seed")
                .and_then(|s| s.parse::<u64>().ok());
                
            // Parse ts_prop_us parameter specific to RelayMBNtserver
            let ts_prop_us = params
                .get("ts_prop_us")
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_micros)
                .unwrap_or(Duration::from_micros(0)); // Default to 0us if not specified
            
            Ok(NodeType::RelayMBNtserver(RelayMBNtserver::new(
                id, edgeside_in_val, edgeside_out_val, machines, Instant::now(),
                max_padding_frac, max_blocking_frac, insecure_rng_seed, ts_prop_us
            )))
        },
        _ => Err(format!("Unknown node type: {}", node_type)),
    }
}



pub fn check_dependent_packets(s_event: &SimulEvent, sq: &mut SimulQueue, outgoing_link: &LinkType, ts_to_relay_extra_us: u64) {
    debug!("\tqueue {:#?} tx_depend check", TriggerEvent::NormalRecv);
    
    if let Some(dependencies) = sq.dependent_tx.remove(&s_event.packet_idx) {
        let link_id = outgoing_link.link_id();

        for (new_pktidx, delta, event_kind) in dependencies {
            debug!("\tqueue tx_depend new_idx: {:#?}   delta: {:#?}   kind: {:#?}", 
                   new_pktidx, delta, event_kind);
            let additional_duration = Duration::from_nanos(delta as u64 + ts_to_relay_extra_us * 1000);
            
            sq.push(SimulEvent {
                event: TriggerEvent::NormalSent,
                time: s_event.time + additional_duration,
                packet_idx: new_pktidx,
                node_idx: s_event.node_idx,
                link_idx: link_id,
                contains_padding: false,
                bypass: false,
                replace: false,
                q_sequence_nr: 0, // Will be overwritten by push()
                #[cfg(debug_assertions)]
                debug_note: None,
            });
        }
    }
}


pub fn make_network_receive_from_sent (s_event: &SimulEvent, _topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
    let new_t_event = match s_event.event {
        TriggerEvent::NormalSent => TriggerEvent::NormalRecv,
        TriggerEvent::TunnelSent => TriggerEvent::TunnelRecv,
        _ => panic!("Unexpected event type: {:?}", s_event.event),
    };
    let link_id = s_event.link_idx;
    
    // Get values we need before mutable borrow
    let to_node = linkstate.links[link_id].to_node();
    let prop_us = linkstate.links[link_id].prop_us();
    
    debug!("\tNode {} sending xxSent -> creating xxRecv at node via link {}", 
            s_event.node_idx, link_id);
    //print s_event time and sq.earliest_event_instant
    //debug!("\ts_event time: {:?}   Earliest event instant: {:?}", s_event.time, sq.earliest_event_instant);
    let current_duration = s_event.time.checked_duration_since(sq.earliest_event_instant)
        .expect(&format!("s_event.time must not be earlier than sq.earliest_event_instant for pkt {:?}", s_event.packet_idx));
    
    // Now we can safely do the mutable borrow for sampling
    let transmission_delay = linkstate.links[link_id].sample(current_duration);
    
    let recv_s_event = SimulEvent {
        event: new_t_event,
        time: s_event.time + transmission_delay + prop_us,
        packet_idx: s_event.packet_idx,
        node_idx: to_node,
        link_idx: link_id,
        contains_padding: s_event.contains_padding,
        bypass: false,
        replace: false,
        q_sequence_nr: 0, // Will be overwritten by push()
        #[cfg(debug_assertions)]
        debug_note: None, 
    };
    sq.push(recv_s_event);
}



pub fn forward_network_receive_from_receive (s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
    let new_t_event = match s_event.event {
        TriggerEvent::NormalRecv => TriggerEvent::NormalRecv,
        TriggerEvent::TunnelRecv => TriggerEvent::TunnelRecv,
        _ => panic!("Unexpected event type: {:?}", s_event.event),
    };

    let outgoing_link_idx = topology.routes[s_event.node_idx][s_event.link_idx].unwrap_or_else(|| {
        panic!("No outgoing link found for node {} with link index {}", s_event.node_idx, s_event.link_idx);
    });
    
    // Get immutable data first
    let to_node = linkstate.links[outgoing_link_idx].to_node();
    let link_id = linkstate.links[outgoing_link_idx].link_id();
    let prop_us = linkstate.links[outgoing_link_idx].prop_us();
    
    // Calculate timing
    let current_duration = s_event.time.checked_duration_since(sq.earliest_event_instant)
        .expect("s_event.time must not be earlier than sq.earliest_event_instant");
    
    // Now do the mutable borrow for sampling
    let transmission_delay = linkstate.links[outgoing_link_idx].sample(current_duration);
    
    debug!("\tForwarding from node {} via link {} to node {}", 
           s_event.node_idx, link_id, to_node);
    
    let recv_s_event = SimulEvent {
        event: new_t_event,
        time: s_event.time + transmission_delay + prop_us,
        packet_idx: s_event.packet_idx,
        node_idx: to_node,
        link_idx: outgoing_link_idx,
        contains_padding: s_event.contains_padding,
        bypass: false,
        replace: false,
        q_sequence_nr: 0, // Will be overwritten by push()
        #[cfg(debug_assertions)]
        debug_note: None, 
    };
    sq.push(recv_s_event);
}



#[derive(Debug, Copy, Clone)]
pub struct ClientBasic {
    pub id: usize,
    coreside_out: usize,
}


impl ClientBasic {
    pub fn new(id: usize, coreside_out: usize) -> Self {
        Self {
            id,
            coreside_out,
        }
    }



    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalSent => {
                make_network_receive_from_sent(s_event, topology, linkstate, sq);
            }
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_idx].get_coreside_out_id();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(s_event, sq, outgoing_link, 0);
            }
            _ => {
                panic!("ClientBasic cannot handle s_event: {:?}", s_event.event);
            }
        }
    }

    pub fn node_id(&self) -> usize {
        self.id
    }

    pub fn get_coreside_out_id(&self) -> usize {
        self.coreside_out
    }

}

#[derive(Debug, Copy, Clone)]
pub struct RouterBasic {
    pub id: usize,
    pub coreside_out: usize,
    pub edgeside_in: usize,
    pub edgeside_out: usize,
}

impl RouterBasic {
    pub fn new(id: usize, coreside_out: usize, edgeside_in: usize, edgeside_out: usize) -> Self {
        Self {
            id,
            coreside_out,
            edgeside_in,
            edgeside_out,
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalRecv => {
                forward_network_receive_from_receive(s_event, topology, linkstate, sq);
            }
            TriggerEvent::PaddingSent { .. } | TriggerEvent::PaddingRecv => {
                // Relay handles padding traffic
            }
            _ => {
                panic!("RouterBasic cannot handle s_event: {:?}", s_event.event);
            }
        }
    }

    pub fn node_id(&self) -> usize {
        self.id
    }

    pub fn get_coreside_out_id(&self) -> usize {
        self.coreside_out
    }

    pub fn get_edgeside_out_id(&self) -> usize {
        self.edgeside_out
    }

    pub fn get_edgeside_in_id(&self) -> usize {
        self.edgeside_in
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TrafficServerBasic {
    pub id: usize,
    pub edgeside_out: usize,
}

impl TrafficServerBasic {
    pub fn new(id: usize, edgeside_out: usize) -> Self {
        Self {
            id,
            edgeside_out,
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_idx].get_edgeside_out_id();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(s_event, sq, outgoing_link, 0);
            }
            TriggerEvent::NormalSent => {
                make_network_receive_from_sent(s_event, topology, linkstate, sq);
            }
            _ => {
                panic!("TrafficServerBasic cannot handle s_event: {:?}", s_event.event);
            }
        }
    }

    pub fn node_id(&self) -> usize {
        self.id
    }

    pub fn get_edgeside_out_id(&self) -> usize {
        self.edgeside_out
    }

    // These methods are required for NodeType enum dispatch
    pub fn get_coreside_linkid(&self) -> usize {
        panic!("TrafficServerBasic does not have a coreside link")
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        self.edgeside_out
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_basic_creation() {
        let client = ClientBasic::new(1, 0);
        assert_eq!(client.node_id(), 1);
    }

    #[test]
    fn test_router_basic_creation() {
        let router = RouterBasic::new(2, 0, 1, 1);
        assert_eq!(router.node_id(), 2);
    }

    #[test]
    fn test_traffic_server_basic_creation() {
        let server = TrafficServerBasic::new(3, 0);
        assert_eq!(server.node_id(), 3);
    }

    #[test]
    fn test_client_mbn_creation() {
        use std::time::Instant;
        let client = ClientMBN::new(4, 2, vec![], Instant::now(), 0.0, 0.0, None);
        assert_eq!(client.node_id(), 4);
        assert_eq!(client.get_coreside_out_id(), 2);
    }

    #[test]
    fn test_relay_mbn_creation() {
        use std::time::Instant;
        let relay = RelayMBN::new(5, 2, 3, 3, vec![], Instant::now(), 0.0, 0.0, None);
        assert_eq!(relay.node_id(), 5);
        assert_eq!(relay.get_coreside_out_id(), 2);
        assert_eq!(relay.get_edgeside_out_id(), 3);
    }

    #[test]
    fn test_node_factory() {
        use std::collections::HashMap;
        
        let empty_params = HashMap::new();
        
        let client = create_node("ClientBasic", 1, Some(0), None, None, &empty_params).unwrap();
        assert_eq!(client.node_id(), 1);
        assert_eq!(client.type_name(), "ClientBasic");

        let router = create_node("RouterBasic", 2, Some(0), Some(1), Some(1), &empty_params).unwrap();
        assert_eq!(router.node_id(), 2);
        assert_eq!(router.type_name(), "RouterBasic");

        let server = create_node("TrafficServerBasic", 3, None, None, Some(0), &empty_params).unwrap();
        assert_eq!(server.node_id(), 3);
        assert_eq!(server.type_name(), "TrafficServerBasic");

        // Test new MBN node types - these require current_time parameter
        let mut mbn_params = HashMap::new();
        mbn_params.insert("current_time".to_string(), "0".to_string()); // 0 nanoseconds from now
        
        let client_mbn = create_node("ClientMBN", 4, Some(2), None, None, &mbn_params).unwrap();
        assert_eq!(client_mbn.node_id(), 4);
        assert_eq!(client_mbn.type_name(), "ClientMBN");

        let relay_mbn = create_node("RelayMBN", 5, Some(2), Some(3), Some(3), &mbn_params).unwrap();
        assert_eq!(relay_mbn.node_id(), 5);
        assert_eq!(relay_mbn.type_name(), "RelayMBN");

        let invalid = create_node("InvalidType", 6, None, None, None, &empty_params);
        assert!(invalid.is_err());
    }
}