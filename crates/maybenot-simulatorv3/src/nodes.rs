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
    RouterBasic(RouterBasic),
    TrafficServerBasic(TrafficServerBasic),
    ClientMBN(ClientMBN),
    RelayMBN(RelayMBN),
}

impl NodeType {
    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match self {
            NodeType::ClientBasic(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::RouterBasic(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::TrafficServerBasic(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::ClientMBN(node) => node.handle_event(s_event, topology, linkstate, sq),
            NodeType::RelayMBN(node) => node.handle_event(s_event, topology, linkstate, sq),
        }
    }

    pub fn node_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.node_id(),
            NodeType::RouterBasic(node) => node.node_id(),
            NodeType::TrafficServerBasic(node) => node.node_id(),
            NodeType::ClientMBN(node) => node.node_id(),
            NodeType::RelayMBN(node) => node.node_id(),
        }
    }

    pub fn get_coreside_linkid(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.get_coreside_linkid(),
            NodeType::RouterBasic(node) => node.get_coreside_linkid(),
            NodeType::TrafficServerBasic(node) => node.get_coreside_linkid(),
            NodeType::ClientMBN(node) => node.get_coreside_linkid(),
            NodeType::RelayMBN(node) => node.get_coreside_linkid(),
        }
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.get_edgeside_linkid(),
            NodeType::RouterBasic(node) => node.get_edgeside_linkid(),
            NodeType::TrafficServerBasic(node) => node.get_edgeside_linkid(),
            NodeType::ClientMBN(node) => node.get_edgeside_linkid(),
            NodeType::RelayMBN(node) => node.get_edgeside_linkid(),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            NodeType::ClientBasic(_) => "ClientBasic",
            NodeType::RouterBasic(_) => "RouterBasic",
            NodeType::TrafficServerBasic(_) => "TrafficServerBasic",
            NodeType::ClientMBN(_) => "ClientMBN",
            NodeType::RelayMBN(_) => "RelayMBN",
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
        "RouterBasic" => {
            let coreside = coreside_link.ok_or_else(|| NodeError::ProcessingError("RouterBasic requires coreside_link".to_string()))?;
            let edgeside = edgeside_link.ok_or_else(|| NodeError::ProcessingError("RouterBasic requires edgeside_link".to_string()))?;
            Ok(NodeType::RouterBasic(RouterBasic::new(id, coreside, edgeside)))
        },
        "TrafficServerBasic" => {
            let edgeside = edgeside_link.ok_or_else(|| NodeError::ProcessingError("TrafficServerBasic requires edgeside_link".to_string()))?;
            Ok(NodeType::TrafficServerBasic(TrafficServerBasic::new(id, edgeside)))
        },
        "ClientMBN" => {
            let coreside = coreside_link.ok_or_else(|| NodeError::ProcessingError("ClientMBN requires coreside_link".to_string()))?;
            Ok(NodeType::ClientMBN(ClientMBN::new(id, coreside)))
        },
        "RelayMBN" => {
            let coreside = coreside_link.ok_or_else(|| NodeError::ProcessingError("RelayMBN requires coreside_link".to_string()))?;
            let edgeside = edgeside_link.ok_or_else(|| NodeError::ProcessingError("RelayMBN requires edgeside_link".to_string()))?;
            Ok(NodeType::RelayMBN(RelayMBN::new(id, coreside, edgeside)))
        },
        _ => Err(NodeError::ProcessingError(format!(
            "Unknown node type: {}", node_type
        ))),
    }
}



fn check_dependent_packets(s_event: &SimulEvent, sq: &mut SimulQueue, outgoing_link: &LinkType) {
    debug!("\tqueue {:#?} tx_depend check", TriggerEvent::NormalRecv);
    
    if let Some(dependencies) = sq.dependent_tx.remove(&s_event.packet_idx) {
        let link_id = outgoing_link.link_id();
        
        for (new_pktidx, delta, event_kind) in dependencies {
            debug!("\tqueue tx_depend new_idx: {:#?}   delta: {:#?}   kind: {:#?}", 
                   new_pktidx, delta, event_kind);
            
            sq.push(SimulEvent {
                event: TriggerEvent::NormalSent,
                time: s_event.time + Duration::from_nanos(delta as u64),
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


fn make_network_receive_from_sent (s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
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



fn forward_network_receive_from_receive (s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
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
    coreside_link: usize,
}


impl ClientBasic {
    pub fn new(id: usize, coreside_link: usize) -> Self {
        Self {
            id,
            coreside_link,
        }
    }



    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalSent => {
                make_network_receive_from_sent(s_event, topology, linkstate, sq);
            }
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_idx].get_coreside_linkid();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(s_event, sq, outgoing_link);
            }
            _ => {
                panic!("ClientBasic cannot handle s_event: {:?}", s_event.event);
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
pub struct RouterBasic {
    pub id: usize,
    pub coreside_link: usize,
    pub edgeside_link: usize,
}

impl RouterBasic {
    pub fn new(id: usize, coreside_link: usize, edgeside_link: usize) -> Self {
        Self {
            id,
            coreside_link,
            edgeside_link,
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

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_idx].get_edgeside_linkid();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(s_event, sq, outgoing_link);
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

    pub fn get_coreside_linkid(&self) -> usize {
        panic!("TrafficServerBasic does not have a coreside link")
    }

    pub fn get_edgeside_linkid(&self) -> usize {
        self.edgeside_link
    }
}

// MBN (Maybenot) node types - initially behave like Basic nodes but designed for future MBN integration

#[derive(Debug, Copy, Clone)]
pub struct ClientMBN {
    pub id: usize,
    coreside_link: usize,
}

impl ClientMBN {
    pub fn new(id: usize, coreside_link: usize) -> Self {
        Self {
            id,
            coreside_link,
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalSent => {
                let forward_s_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time, 
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
                    contains_padding: false,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_s_event);
            }
            


            TriggerEvent::TunnelSent => {
                make_network_receive_from_sent(s_event, topology, linkstate, sq);
            }


            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time, 
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
                    contains_padding: true,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_s_event);
            }



            TriggerEvent::TunnelRecv => {
                let new_t_event = match &s_event.contains_padding {
                    true => {
                        TriggerEvent::PaddingRecv
                    },
                    false => {
                        TriggerEvent::NormalRecv
                    }
                };
                let forward_s_event = SimulEvent {
                    event: new_t_event,
                    time: s_event.time, 
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
                    contains_padding: s_event.contains_padding,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_s_event);
            }


            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_idx].get_coreside_linkid();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(s_event, sq, outgoing_link);
            }
            TriggerEvent::PaddingRecv => {}
            TriggerEvent::BlockingBegin { machine } => {}
            TriggerEvent::BlockingEnd => {}

            _ => {
                panic!("ClientMBN cannot handle s_event: {:?}", s_event.event);
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
        panic!("ClientMBN does not have an edgeside link")
    }
}

#[derive(Debug, Copy, Clone)]
pub struct RelayMBN {
    pub id: usize,
    pub coreside_link: usize,
    pub edgeside_link: usize,
}

impl RelayMBN {
    pub fn new(id: usize, coreside_link: usize, edgeside_link: usize) -> Self {
        Self {
            id,
            coreside_link,
            edgeside_link,
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::TunnelRecv => {
                let new_event = match &s_event.contains_padding {
                    true => {
                        TriggerEvent::PaddingRecv
                    },
                    false => {
                        TriggerEvent::NormalRecv
                    }
                };
                let forward_event = SimulEvent {
                    event: new_event,
                    time: s_event.time,
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx,
                    link_idx: s_event.link_idx,
                    contains_padding: false,
                    bypass: false,
                    replace: false,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_event);
            }
            TriggerEvent::NormalRecv => {
                let outlink = topology.get_outlink(s_event.node_idx, s_event.link_idx).unwrap();
                if  outlink == self.coreside_link {
                    forward_network_receive_from_receive(s_event, topology, linkstate, sq);
                } else if outlink == self.edgeside_link {
                    let new_s_event = SimulEvent {
                        event: TriggerEvent::NormalSent,
                        time: s_event.time,
                        packet_idx: s_event.packet_idx,
                        node_idx: s_event.node_idx,
                        link_idx: outlink,
                        contains_padding: false,
                        bypass: false,
                        replace: false,
                        q_sequence_nr: 0, // Will be overwritten by push()
                        #[cfg(debug_assertions)]
                        debug_note: None, 
                    };
                    sq.push(new_s_event);
                } else {
                    panic!("RelayMBN received NormalRecv on unexpected link index: {}", s_event.link_idx);
                }
            }

            TriggerEvent::NormalSent => {
                if  s_event.link_idx == self.coreside_link {
                    make_network_receive_from_sent(s_event, topology, linkstate, sq);
                } else if s_event.link_idx == self.edgeside_link {
                    let forward_s_event = SimulEvent {
                        event: TriggerEvent::TunnelSent,
                        time: s_event.time, 
                        packet_idx: s_event.packet_idx,
                        node_idx: s_event.node_idx, 
                        link_idx: s_event.link_idx, 
                        contains_padding: false,
                        bypass: s_event.bypass,
                        replace: s_event.replace,
                        q_sequence_nr: 0, // Will be overwritten by push()
                        #[cfg(debug_assertions)]
                        debug_note: None,
                    };
                    sq.push(forward_s_event);
                } else {
                    panic!("RelayMBN received NormalRecv on unexpected link index: {}", s_event.link_idx);
                }
            }


            TriggerEvent::TunnelSent => {
                make_network_receive_from_sent(s_event, topology, linkstate, sq);
            }


            TriggerEvent::PaddingSent { .. } => {
                let forward_s_event = SimulEvent {
                    event: TriggerEvent::TunnelSent,
                    time: s_event.time, 
                    packet_idx: s_event.packet_idx,
                    node_idx: s_event.node_idx, 
                    link_idx: s_event.link_idx, 
                    contains_padding: true,
                    bypass: s_event.bypass,
                    replace: s_event.replace,
                    q_sequence_nr: 0, // Will be overwritten by push()
                    #[cfg(debug_assertions)]
                    debug_note: None,
                };
                sq.push(forward_s_event);
            }
            TriggerEvent::PaddingRecv => {}
            TriggerEvent::BlockingBegin { machine } => {}
            TriggerEvent::BlockingEnd => {}
    
            _ => {
                panic!("RelayMBN cannot handle s_event: {:?}", s_event.event);
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
        let router = RouterBasic::new(2, 0, 1);
        assert_eq!(router.node_id(), 2);
    }

    #[test]
    fn test_traffic_server_basic_creation() {
        let server = TrafficServerBasic::new(3, 0);
        assert_eq!(server.node_id(), 3);
    }

    #[test]
    fn test_client_mbn_creation() {
        let client = ClientMBN::new(4, 2);
        assert_eq!(client.node_id(), 4);
        assert_eq!(client.get_coreside_linkid(), 2);
    }

    #[test]
    fn test_relay_mbn_creation() {
        let relay = RelayMBN::new(5, 2, 3);
        assert_eq!(relay.node_id(), 5);
        assert_eq!(relay.get_coreside_linkid(), 2);
        assert_eq!(relay.get_edgeside_linkid(), 3);
    }

    #[test]
    fn test_node_factory() {
        let client = create_node("ClientBasic", 1, Some(0), None).unwrap();
        assert_eq!(client.node_id(), 1);
        assert_eq!(client.type_name(), "ClientBasic");

        let router = create_node("RouterBasic", 2, Some(0), Some(1)).unwrap();
        assert_eq!(router.node_id(), 2);
        assert_eq!(router.type_name(), "RouterBasic");

        let server = create_node("TrafficServerBasic", 3, None, Some(0)).unwrap();
        assert_eq!(server.node_id(), 3);
        assert_eq!(server.type_name(), "TrafficServerBasic");

        // Test new MBN node types
        let client_mbn = create_node("ClientMBN", 4, Some(2), None).unwrap();
        assert_eq!(client_mbn.node_id(), 4);
        assert_eq!(client_mbn.type_name(), "ClientMBN");

        let relay_mbn = create_node("RelayMBN", 5, Some(2), Some(3)).unwrap();
        assert_eq!(relay_mbn.node_id(), 5);
        assert_eq!(relay_mbn.type_name(), "RelayMBN");

        let invalid = create_node("InvalidType", 6, None, None);
        assert!(invalid.is_err());
    }
}