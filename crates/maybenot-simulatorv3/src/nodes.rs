use maybenot::TriggerEvent;
use crate::{SimulEvent, SimulQueue};
use crate::network::{NetworkTopology, NetworkLinkstate};
use crate::links::LinkType;
use std::time::Duration;
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
#[derive(Debug, Clone)]
pub enum NodeType {
    ClientBasic(ClientBasic),
    RelayBasic(RelayBasic),
    TrafficServerBasic(TrafficServerBasic),
}

impl NodeType {
    pub fn handle_event(&self, event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match self {
            NodeType::ClientBasic(node) => node.handle_event(event, topology, linkstate, sq),
            NodeType::RelayBasic(node) => node.handle_event(event, topology, linkstate, sq),
            NodeType::TrafficServerBasic(node) => node.handle_event(event, topology, linkstate, sq),
        }
    }

    pub fn node_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.node_id(),
            NodeType::RelayBasic(node) => node.node_id(),
            NodeType::TrafficServerBasic(node) => node.node_id(),
        }
    }

    pub fn get_coreside_linkid(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.get_coreside_linkid(),
            NodeType::RelayBasic(node) => node.get_coreside_linkid(),
            NodeType::TrafficServerBasic(node) => node.get_coreside_linkid(),
        }
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.get_edgeside_linkid(),
            NodeType::RelayBasic(node) => node.get_edgeside_linkid(),
            NodeType::TrafficServerBasic(node) => node.get_edgeside_linkid(),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            NodeType::ClientBasic(_) => "ClientBasic",
            NodeType::RelayBasic(_) => "RelayBasic",
            NodeType::TrafficServerBasic(_) => "TrafficServerBasic",
        }
    }
}

// Factory function for creating nodes from TOML configuration
pub fn create_node(node_type: &str, id: usize, coreside_link: Option<usize>, edgeside_link: Option<usize>) -> Result<NodeType, NodeError> {
    match node_type {
        "ClientBasic" => {
            let coreside = coreside_link.ok_or_else(|| NodeError::ProcessingError("ClientBasic requires coreside_link".to_string()))?;
            Ok(NodeType::ClientBasic(ClientBasic::new(id, coreside)))
        },
        "RelayBasic" => {
            let coreside = coreside_link.ok_or_else(|| NodeError::ProcessingError("RelayBasic requires coreside_link".to_string()))?;
            let edgeside = edgeside_link.ok_or_else(|| NodeError::ProcessingError("RelayBasic requires edgeside_link".to_string()))?;
            Ok(NodeType::RelayBasic(RelayBasic::new(id, coreside, edgeside)))
        },
        "TrafficServerBasic" => {
            let edgeside = edgeside_link.ok_or_else(|| NodeError::ProcessingError("TrafficServerBasic requires edgeside_link".to_string()))?;
            Ok(NodeType::TrafficServerBasic(TrafficServerBasic::new(id, edgeside)))
        },
        _ => Err(NodeError::ProcessingError(format!(
            "Unknown node type: {}", node_type
        ))),
    }
}



fn check_dependent_packets(event: &SimulEvent, sq: &mut SimulQueue, outgoing_link: &LinkType) {
    debug!("\tqueue {:#?} tx_depend check", TriggerEvent::NormalRecv);
    
    if let Some(dependencies) = sq.dependent_tx.remove(&event.packet_idx) {
        let link_id = outgoing_link.link_id();
        
        for (new_pktidx, delta, event_kind) in dependencies {
            debug!("\tqueue tx_depend new_idx: {:#?}   delta: {:#?}   kind: {:#?}", 
                   new_pktidx, delta, event_kind);
            
            sq.push(SimulEvent {
                event: TriggerEvent::NormalSent,
                time: event.time + Duration::from_nanos(delta as u64),
                packet_idx: new_pktidx,
                node_idx: event.node_idx,
                link_idx: link_id,
                contains_padding: false,
                bypass: false,
                replace: false,
                #[cfg(debug_assertions)]
                debug_note: None,
            });
        }
    }
}


fn make_network_receive_from_sent (event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
    let link_id = if event.node_idx == topology.client {
        topology.nodes[event.node_idx].get_coreside_linkid()
    } else if event.node_idx == topology.traffic_server {
        topology.nodes[event.node_idx].get_edgeside_linkid()
    } else {
        panic!("Node {} is neither client nor traffic server", event.node_idx);
    };
    
    // Get values we need before mutable borrow
    let to_node = linkstate.links[link_id].to_node();
    let prop_ms = linkstate.links[link_id].prop_ms();
    
    debug!("\tClient {} sending NormalSent -> creating NormalRecv at node via link {}", 
            event.node_idx, link_id);
    let current_duration = event.time.checked_duration_since(sq.earliest_event_instant)
        .expect("event.time must not be earlier than sq.earliest_event_instant");
    
    // Now we can safely do the mutable borrow for sampling
    let transmission_delay = linkstate.links[link_id].sample(current_duration);
    
    let recv_event = SimulEvent {
        event: TriggerEvent::NormalRecv,
        time: event.time + transmission_delay + prop_ms,
        packet_idx: event.packet_idx,
        node_idx: to_node,
        link_idx: link_id,
        contains_padding: false,
        bypass: false,
        replace: false,
        #[cfg(debug_assertions)]
        debug_note: None, 
    };
    sq.push(recv_event);
}



fn forward_network_receive_from_receive (event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
    
    let outgoing_link_idx = topology.routes[event.node_idx][event.link_idx].unwrap_or_else(|| {
        panic!("No outgoing link found for node {} with link index {}", event.node_idx, event.link_idx);
    });
    
    // Get immutable data first
    let to_node = linkstate.links[outgoing_link_idx].to_node();
    let link_id = linkstate.links[outgoing_link_idx].link_id();
    let prop_ms = linkstate.links[outgoing_link_idx].prop_ms();
    
    // Calculate timing
    let current_duration = event.time.checked_duration_since(sq.earliest_event_instant)
        .expect("event.time must not be earlier than sq.earliest_event_instant");
    
    // Now do the mutable borrow for sampling
    let transmission_delay = linkstate.links[outgoing_link_idx].sample(current_duration);
    
    debug!("\tForwarding from node {} via link {} to node {}", 
           event.node_idx, link_id, to_node);
    
    let recv_event = SimulEvent {
        event: TriggerEvent::NormalRecv,
        time: event.time + transmission_delay + prop_ms,
        packet_idx: event.packet_idx,
        node_idx: to_node,
        link_idx: outgoing_link_idx,
        contains_padding: event.contains_padding,
        bypass: false,
        replace: false,
        #[cfg(debug_assertions)]
        debug_note: None, 
    };
    sq.push(recv_event);
}



#[derive(Debug, Copy, Clone)]
pub struct ClientBasic {
    pub id: usize,
    coreside_link: usize,
}


impl ClientBasic {
    pub fn new(id: usize, coreside_link: usize) -> Self {
        Self {
            id,
            coreside_link,
        }
    }



    pub fn handle_event(&self, event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &event.event {
            TriggerEvent::NormalSent => {
                make_network_receive_from_sent(event, topology, linkstate, sq);
            }
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[event.node_idx].get_coreside_linkid();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(event, sq, outgoing_link);
            }
            _ => {
                panic!("ClientBasic cannot handle event: {:?}", event.event);
            }
        }
    }

    pub fn node_id(&self) -> usize {
        self.id
    }

    pub fn get_coreside_linkid(&self) -> usize {
        self.coreside_link
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        panic!("ClientBasic does not have an edgeside link")
    }
}

#[derive(Debug, Copy, Clone)]
pub struct RelayBasic {
    pub id: usize,
    pub coreside_link: usize,
    pub edgeside_link: usize,
}

impl RelayBasic {
    pub fn new(id: usize, coreside_link: usize, edgeside_link: usize) -> Self {
        Self {
            id,
            coreside_link,
            edgeside_link,
        }
    }

    pub fn handle_event(&self, event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &event.event {
            TriggerEvent::TunnelRecv => {
                let forward_event = SimulEvent {
                    event: TriggerEvent::NormalSent,
                    time: event.time + std::time::Duration::from_micros(100), // Small processing delay
                    packet_idx: event.packet_idx,
                    node_idx: self.id, // This relay
                    link_idx: 0, // TODO: proper link management
                    contains_padding: event.contains_padding,
                    bypass: false,
                    replace: false,
                    #[cfg(debug_assertions)]
                debug_note: None,
                };
                sq.push(forward_event);
            }
            TriggerEvent::NormalRecv => {
                forward_network_receive_from_receive(event, topology, linkstate, sq);
            }
            TriggerEvent::PaddingSent { .. } | TriggerEvent::PaddingRecv => {
                // Relay handles padding traffic
            }
            _ => {
                panic!("RelayBasic cannot handle event: {:?}", event.event);
            }
        }
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

#[derive(Debug, Copy, Clone)]
pub struct TrafficServerBasic {
    pub id: usize,
    pub edgeside_link: usize,
}

impl TrafficServerBasic {
    pub fn new(id: usize, edgeside_link: usize) -> Self {
        Self {
            id,
            edgeside_link,
        }
    }

    pub fn handle_event(&self, event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &event.event {
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[event.node_idx].get_edgeside_linkid();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(event, sq, outgoing_link);
            }
            TriggerEvent::NormalSent => {
                make_network_receive_from_sent(event, topology, linkstate, sq);
            }
            _ => {
                panic!("TrafficServerBasic cannot handle event: {:?}", event.event);
            }
        }
    }

    pub fn node_id(&self) -> usize {
        self.id
    }

    pub fn get_coreside_linkid(&self) -> usize {
        panic!("TrafficServerBasic does not have a coreside link")
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        self.edgeside_link
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
    fn test_relay_basic_creation() {
        let relay = RelayBasic::new(2, 0, 1);
        assert_eq!(relay.node_id(), 2);
    }

    #[test]
    fn test_traffic_server_basic_creation() {
        let server = TrafficServerBasic::new(3, 0);
        assert_eq!(server.node_id(), 3);
    }

    #[test]
    fn test_node_factory() {
        let client = create_node("ClientBasic", 1, Some(0), None).unwrap();
        assert_eq!(client.node_id(), 1);
        assert_eq!(client.type_name(), "ClientBasic");

        let relay = create_node("RelayBasic", 2, Some(0), Some(1)).unwrap();
        assert_eq!(relay.node_id(), 2);
        assert_eq!(relay.type_name(), "RelayBasic");

        let server = create_node("TrafficServerBasic", 3, None, Some(0)).unwrap();
        assert_eq!(server.node_id(), 3);
        assert_eq!(server.type_name(), "TrafficServerBasic");

        let invalid = create_node("InvalidType", 4, None, None);
        assert!(invalid.is_err());
    }
}