use maybenot::TriggerEvent;
use crate::{SimulEvent, EventKind, SimulQueue};
use crate::network::Network;
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



#[derive(Debug, Clone)]
pub struct ClientBasic {
    pub id: usize,
    pub coreside_link: usize,
}

impl ClientBasic {
    pub fn new(id: usize, coreside_link: usize) -> Self {
        Self {
            id,
            coreside_link,
        }
    }

    pub fn handle_event(&self, event: &SimulEvent, network: &Network, sq: &mut SimulQueue) -> Result<Vec<SimulEvent>, NodeError> {
        
        let mut response_events = Vec::new();
        
        match &event.event {
            TriggerEvent::NormalSent => {
                
                let outgoing_link = &network.links[network.nodes[event.node_idx].get_coreside_linkid()];
                
                debug!("\tClient {} sending NormalSent -> creating NormalRecv at node via link {}", 
                       self.id, outgoing_link.link_id());
                
                let recv_event = SimulEvent {
                    event: TriggerEvent::NormalRecv,
                    time: event.time,  // FIXME: current_time + time from link.sample() + propagation delay
                    packet_idx: event.packet_idx,
                    node_idx: outgoing_link.to_node(),
                    link_idx: outgoing_link.link_id(),
                    contains_padding: false,
                    bypass: false,
                    replace: false,
                    debug_note: None, 
                };
                response_events.push(recv_event);
            }
            TriggerEvent::NormalRecv => {

                debug!("\tqueue {:#?} tx_depend check", TriggerEvent::NormalRecv);
                let dependent_events = sq.dependent_tx.get(&event.packet_idx);
                if dependent_events.is_some() {
                    let outgoing_link = &network.links[network.nodes[event.node_idx].get_coreside_linkid()];

                    // We have dependent packets, so we need to queue them up. Apply cloning for now, unoptimized
                    for (new_pktidx, delta, event_kind) in dependent_events.unwrap().clone() {
                        debug!("\tqueue tx_depend new_idx: {:#?}   delta: {:#?}   kind: {:#?} ", new_pktidx, delta, event_kind);
                        // We are at client, and we send toward webserver
                        sq.push(SimulEvent {
                            event: TriggerEvent::NormalSent,
                            time: event.time,  // FIXME: current_time + time from link.sample() + propagation delay
                            //integration_delay: next.integration_delay,
                            packet_idx: new_pktidx,
                            node_idx: outgoing_link.to_node(),
                            link_idx: outgoing_link.link_id(),
                            contains_padding: false,
                            bypass: false,
                            replace: false,
                            debug_note: None,
                        });
                    }
                }
            }
            _ => {
                return Err(NodeError::InvalidEvent(format!(
                    "ClientBasic cannot handle event: {:?}", event.event
                )));
            }
        }
        Ok(response_events)
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

#[derive(Debug, Clone)]
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

    pub fn handle_event(&self, event: &SimulEvent, network: &Network, sq: &mut SimulQueue) -> Result<Vec<SimulEvent>, NodeError> {
        
        let mut response_events = Vec::new();
        
        match &event.event {
            TriggerEvent::NormalRecv => {
                // Relay received from client tunnel - forward to server
                
                let forward_event = SimulEvent {
                    event: TriggerEvent::NormalSent,
                    time: event.time + std::time::Duration::from_micros(100), // Small processing delay
                    packet_idx: event.packet_idx,
                    node_idx: self.id, // This relay
                    link_idx: 0, // TODO: proper link management
                    contains_padding: event.contains_padding,
                    bypass: false,
                    replace: false,
                    debug_note: None,
                };
                response_events.push(forward_event);
            }
            TriggerEvent::NormalRecv => {
                // Relay received from server - forward to client tunnel
                
                let forward_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: event.time + std::time::Duration::from_micros(100),
                    packet_idx: event.packet_idx,
                    node_idx: self.id,
                    link_idx: 0,
                    contains_padding: event.contains_padding,
                    bypass: false,
                    replace: false,
                    debug_note: Some("Relay forwarding to client".to_string()),
                };
                response_events.push(forward_event);
            }
            TriggerEvent::PaddingSent { .. } | TriggerEvent::PaddingRecv => {
                // Relay handles padding traffic
            }
            _ => {
                return Err(NodeError::InvalidEvent(format!(
                    "RelayBasic cannot handle event: {:?}", event.event
                )));
            }
        }
        
        Ok(response_events)
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

#[derive(Debug, Clone)]
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

    pub fn handle_event(&self, event: &SimulEvent, network: &Network, sq: &mut SimulQueue) -> Result<Vec<SimulEvent>, NodeError> {
        
        let mut response_events = Vec::new();
        
        match &event.event {
            TriggerEvent::NormalRecv => {
                // Traffic server received a request - send a response
                
                let response_event = SimulEvent {
                    event: TriggerEvent::NormalSent,
                    time: event.time + std::time::Duration::from_millis(1), // Server processing delay
                    packet_idx: event.packet_idx + 1000, // Different packet ID for response
                    node_idx: self.id,
                    link_idx: 0,
                    contains_padding: false,
                    bypass: false,
                    replace: false,
                    debug_note: Some("Traffic server response".to_string()),
                };
                response_events.push(response_event);
            }
            TriggerEvent::NormalSent => {
                // Traffic server sending (probably a response we generated)
            }
            _ => {
                return Err(NodeError::InvalidEvent(format!(
                    "TrafficServerBasic cannot handle event: {:?}", event.event
                )));
            }
        }
        
        Ok(response_events)
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

// High-performance enum-based node dispatch
#[derive(Debug, Clone)]
pub enum NodeType {
    ClientBasic(ClientBasic),
    RelayBasic(RelayBasic),
    TrafficServerBasic(TrafficServerBasic),
}

impl NodeType {
    pub fn handle_event(&self, event: &SimulEvent, network: &Network, sq: &mut SimulQueue) -> Result<Vec<SimulEvent>, NodeError> {
        match self {
            NodeType::ClientBasic(node) => node.handle_event(event, network, sq),
            NodeType::RelayBasic(node) => node.handle_event(event, network, sq),
            NodeType::TrafficServerBasic(node) => node.handle_event(event, network, sq),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_client_basic_creation() {
        let client = ClientBasic::new(1);
        assert_eq!(client.node_id(), 1);
        assert_eq!(client.packet_count, 0);
    }

    #[test]
    fn test_relay_basic_creation() {
        let relay = RelayBasic::new(2);
        assert_eq!(relay.node_id(), 2);
        assert_eq!(relay.forwarded_count, 0);
    }

    #[test]
    fn test_traffic_server_basic_creation() {
        let server = TrafficServerBasic::new(3);
        assert_eq!(server.node_id(), 3);
        assert_eq!(server.responses_sent, 0);
    }

    #[test]
    fn test_node_factory() {
        let client = create_node("ClientBasic", 1).unwrap();
        assert_eq!(client.node_id(), 1);
        assert_eq!(client.type_name(), "ClientBasic");

        let relay = create_node("RelayBasic", 2).unwrap();
        assert_eq!(relay.node_id(), 2);
        assert_eq!(relay.type_name(), "RelayBasic");

        let server = create_node("TrafficServerBasic", 3).unwrap();
        assert_eq!(server.node_id(), 3);
        assert_eq!(server.type_name(), "TrafficServerBasic");

        let invalid = create_node("InvalidType", 4);
        assert!(invalid.is_err());
    }

    #[test]
    fn test_enum_handle_event() {
        let mut client = NodeType::ClientBasic(ClientBasic::new(1));
        let event = SimulEvent {
            event: TriggerEvent::NormalSent,
            time: Instant::now(),
            packet_idx: 0,
            node_idx: 1,
            link_idx: 0,
            contains_padding: false,
            bypass: false,
            replace: false,
            debug_note: None,
        };

        let result = client.handle_event(&event);
        assert!(result.is_ok());
        assert_eq!(client.packet_count(), 1);
    }

    #[test]
    fn test_traffic_server_response() {
        let mut server = NodeType::TrafficServerBasic(TrafficServerBasic::new(3));
        let event = SimulEvent {
            event: TriggerEvent::NormalRecv,
            time: Instant::now(),
            packet_idx: 100,
            node_idx: 3,
            link_idx: 0,
            contains_padding: false,
            bypass: false,
            replace: false,
            debug_note: None,
        };

        let result = server.handle_event(&event).unwrap();
        assert_eq!(result.len(), 1);
        
        if let NodeType::TrafficServerBasic(ref server_node) = server {
            assert_eq!(server_node.responses_sent, 1);
        }
        
        let response = &result[0];
        assert_eq!(response.packet_idx, 1100); // 100 + 1000
        assert!(matches!(response.event, TriggerEvent::NormalSent));
    }

    #[test]
    fn test_enum_dispatch_performance() {
        // Test that enum dispatch works efficiently
        let mut nodes = vec![
            NodeType::ClientBasic(ClientBasic::new(1)),
            NodeType::RelayBasic(RelayBasic::new(2)),
            NodeType::TrafficServerBasic(TrafficServerBasic::new(3)),
        ];

        let event = SimulEvent {
            event: TriggerEvent::NormalSent,
            time: Instant::now(),
            packet_idx: 0,
            node_idx: 1,
            link_idx: 0,
            contains_padding: false,
            bypass: false,
            replace: false,
            debug_note: None,
        };

        // Process event on all nodes - this should be very fast
        for node in &mut nodes {
            let _ = node.handle_event(&event);
        }

        // Verify all nodes processed the event
        for node in &nodes {
            assert!(node.packet_count() > 0);
        }
    }
}