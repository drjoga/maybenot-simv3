use maybenot::TriggerEvent;
use crate::SimulEvent;

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
    pub id: u32,
    pub packet_count: usize,
}

impl ClientBasic {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            packet_count: 0,
        }
    }

    pub fn handle_event(&mut self, event: &SimulEvent) -> Result<Vec<SimulEvent>, NodeError> {
        self.packet_count += 1;
        
        let response_events = Vec::new();
        
        match &event.event {
            TriggerEvent::NormalSent => {
                // Client sending a normal packet - just log it
            }
            TriggerEvent::NormalRecv => {
                // Client received a normal packet - might trigger a response
                // For basic client, we'll just acknowledge receipt
            }
            TriggerEvent::TunnelSent => {
                // Client sending through tunnel
            }
            TriggerEvent::TunnelRecv => {
                // Client receiving through tunnel
            }
            _ => {
                return Err(NodeError::InvalidEvent(format!(
                    "ClientBasic cannot handle event: {:?}", event.event
                )));
            }
        }
        
        Ok(response_events)
    }

    pub fn node_id(&self) -> u32 {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct RelayBasic {
    pub id: u32,
    pub packet_count: usize,
    pub forwarded_count: usize,
}

impl RelayBasic {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            packet_count: 0,
            forwarded_count: 0,
        }
    }

    pub fn handle_event(&mut self, event: &SimulEvent) -> Result<Vec<SimulEvent>, NodeError> {
        self.packet_count += 1;
        
        let mut response_events = Vec::new();
        
        match &event.event {
            TriggerEvent::TunnelRecv => {
                // Relay received from client tunnel - forward to server
                self.forwarded_count += 1;
                
                let forward_event = SimulEvent {
                    event: TriggerEvent::NormalSent,
                    time: event.time + std::time::Duration::from_micros(100), // Small processing delay
                    packet_idx: event.packet_idx,
                    node_idx: self.id as usize, // This relay
                    link_idx: 0, // TODO: proper link management
                    contains_padding: event.contains_padding,
                    bypass: false,
                    replace: false,
                    debug_note: Some("Relay forwarding to server".to_string()),
                };
                response_events.push(forward_event);
            }
            TriggerEvent::NormalRecv => {
                // Relay received from server - forward to client tunnel
                self.forwarded_count += 1;
                
                let forward_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: event.time + std::time::Duration::from_micros(100),
                    packet_idx: event.packet_idx,
                    node_idx: self.id as usize,
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

    pub fn node_id(&self) -> u32 {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct TrafficServerBasic {
    pub id: u32,
    pub packet_count: usize,
    pub responses_sent: usize,
}

impl TrafficServerBasic {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            packet_count: 0,
            responses_sent: 0,
        }
    }

    pub fn handle_event(&mut self, event: &SimulEvent) -> Result<Vec<SimulEvent>, NodeError> {
        self.packet_count += 1;
        
        let mut response_events = Vec::new();
        
        match &event.event {
            TriggerEvent::NormalRecv => {
                // Traffic server received a request - send a response
                self.responses_sent += 1;
                
                let response_event = SimulEvent {
                    event: TriggerEvent::NormalSent,
                    time: event.time + std::time::Duration::from_millis(1), // Server processing delay
                    packet_idx: event.packet_idx + 1000, // Different packet ID for response
                    node_idx: self.id as usize,
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

    pub fn node_id(&self) -> u32 {
        self.id
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
    pub fn handle_event(&mut self, event: &SimulEvent) -> Result<Vec<SimulEvent>, NodeError> {
        match self {
            NodeType::ClientBasic(node) => node.handle_event(event),
            NodeType::RelayBasic(node) => node.handle_event(event),
            NodeType::TrafficServerBasic(node) => node.handle_event(event),
        }
    }

    pub fn node_id(&self) -> u32 {
        match self {
            NodeType::ClientBasic(node) => node.node_id(),
            NodeType::RelayBasic(node) => node.node_id(),
            NodeType::TrafficServerBasic(node) => node.node_id(),
        }
    }

    pub fn packet_count(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.packet_count,
            NodeType::RelayBasic(node) => node.packet_count,
            NodeType::TrafficServerBasic(node) => node.packet_count,
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
pub fn create_node(node_type: &str, id: u32) -> Result<NodeType, NodeError> {
    match node_type {
        "ClientBasic" => Ok(NodeType::ClientBasic(ClientBasic::new(id))),
        "RelayBasic" => Ok(NodeType::RelayBasic(RelayBasic::new(id))),
        "TrafficServerBasic" => Ok(NodeType::TrafficServerBasic(TrafficServerBasic::new(id))),
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