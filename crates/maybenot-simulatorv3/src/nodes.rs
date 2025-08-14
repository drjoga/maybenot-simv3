use maybenot::TriggerEvent;
use crate::{SimulEvent, SimulInfo, SimulQueue};
use crate::topology::{NetworkTopology, NetworkLinkstate};
use crate::links::LinkType;
use crate::mbn_nodes::{ClientMBN, RelayMBN, RelayMBNtserver};
use std::time::Duration;
use log::debug;


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
    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo, sq: &mut SimulQueue) {
        match self {
            NodeType::ClientBasic(node) => node.handle_event(s_event, topology, linkstate, si, sq),
            NodeType::RouterBasic(node) => node.handle_event(s_event, topology, linkstate, si, sq),
            NodeType::TrafficServerBasic(node) => node.handle_event(s_event, topology, linkstate, si, sq),
            NodeType::ClientMBN(node) => node.handle_event(s_event, topology, linkstate, si, sq),
            NodeType::RelayMBN(node) => node.handle_event(s_event, topology, linkstate, si, sq),
            NodeType::RelayMBNtserver(node) => node.handle_event(s_event, topology, linkstate, si, sq),
        }
    }

    pub fn node_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.id,
            NodeType::RouterBasic(node) => node.id,
            NodeType::TrafficServerBasic(node) => node.id,
            NodeType::ClientMBN(node) => node.id,
            NodeType::RelayMBN(node) => node.id,
            NodeType::RelayMBNtserver(node) => node.id,
        }
    }

    pub fn get_coreside_out_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(node) => node.coreside_out,
            NodeType::RouterBasic(node) => node.coreside_out,
            NodeType::TrafficServerBasic(_) => panic!("TrafficServerBasic does not have a coreside link"),
            NodeType::ClientMBN(node) => node.coreside_out,
            NodeType::RelayMBN(node) => node.coreside_out,
            NodeType::RelayMBNtserver(_) => panic!("RelayMBNtserver does not have a coreside link"),
        }
    }

    pub fn get_edgeside_out_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(_) => panic!("ClientBasic does not have an edgeside link"),
            NodeType::RouterBasic(node) => node.edgeside_out,
            NodeType::TrafficServerBasic(node) => node.edgeside_out,
            NodeType::ClientMBN(_) => panic!("ClientMBN does not have an edgeside link"),
            NodeType::RelayMBN(node) => node.edgeside_out,
            NodeType::RelayMBNtserver(node) => node.edgeside_out,
        }
    }

    pub fn get_edgeside_in_id(&self) -> usize {
        match self {
            NodeType::ClientBasic(_) => panic!("ClientBasic does not have an edgeside link"),
            NodeType::RouterBasic(_) => panic!("RouterBasic does not have edgeside_in"),
            NodeType::TrafficServerBasic(_) => panic!("TrafficServerBasic does not have edgeside_in"),
            NodeType::ClientMBN(_) => panic!("ClientMBN does not have an edgeside link"),
            NodeType::RelayMBN(node) => node.edgeside_in,
            NodeType::RelayMBNtserver(node) => node.edgeside_in,
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


pub fn check_dependent_packets(s_event: &SimulEvent, si: &SimulInfo, sq: &mut SimulQueue, outgoing_link: &LinkType, ts_to_relay_extra_us: u64) {
    debug!("\tqueue {:#?} tx_depend check", TriggerEvent::NormalRecv);
    
    if  !si.dependent_tx[s_event.packet_id].is_empty() {
        let link_id = outgoing_link.link_id();
        
        for (new_pktidx, delta, event_kind) in &si.dependent_tx[s_event.packet_id] {
            debug!("\tqueue tx_depend new_id: {:#?}   delta: {:#?}   kind: {:#?}", 
                   new_pktidx, delta, event_kind);
            let additional_duration = Duration::from_nanos(*delta as u64 + ts_to_relay_extra_us * 1000);
            
            sq.push(SimulEvent {
                event: TriggerEvent::NormalSent,
                time: s_event.time + additional_duration,
                packet_id: *new_pktidx,
                node_id: s_event.node_id,
                link_id,
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


pub fn make_network_receive_from_sent (s_event: &SimulEvent, _topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo, sq: &mut SimulQueue) {
    let new_t_event = match s_event.event {
        TriggerEvent::NormalSent => TriggerEvent::NormalRecv,
        TriggerEvent::TunnelSent => TriggerEvent::TunnelRecv,
        _ => panic!("Unexpected event type: {:?}", s_event.event),
    };
    let link_id = s_event.link_id;
    
    // Get values we need before mutable borrow
    let to_node = linkstate.links[link_id].to_node();
    let current_duration = s_event.time.checked_duration_since(si.earliest_event_instant)
        .unwrap_or_else(|| panic!("s_event.time must not be earlier than si.earliest_event_instant for pkt {:?}", s_event.packet_id));
    let prop_us = if linkstate.links[link_id].fixed_propagation() {
        linkstate.links[link_id].get_prop_us_fixed()
    } else {
        let current_time_ms = current_duration.as_millis() as usize;
        linkstate.links[link_id].get_prop_us_variable(current_time_ms)
    };
    
    debug!("\tNode {} sending xxSent -> creating xxRecv at node via link {}", 
            s_event.node_id, link_id);
    //print s_event time and sq.earliest_event_instant
    //debug!("\ts_event time: {:?}   Earliest event instant: {:?}", s_event.time, sq.earliest_event_instant);
    
    // Now we can safely do the mutable borrow for sampling
    let transmission_delay = linkstate.links[link_id].sample(current_duration);
    
    let recv_s_event = SimulEvent {
        event: new_t_event,
        time: s_event.time + transmission_delay + prop_us,
        packet_id: s_event.packet_id,
        node_id: to_node,
        link_id,
        contains_padding: s_event.contains_padding,
        bypass: false,
        replace: false,
        q_sequence_nr: 0, // Will be overwritten by push()
        #[cfg(debug_assertions)]
        debug_note: None, 
    };
    sq.push(recv_s_event);
}



pub fn forward_network_receive_from_receive (s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo, sq: &mut SimulQueue) {
    let new_t_event = match s_event.event {
        TriggerEvent::NormalRecv => TriggerEvent::NormalRecv,
        TriggerEvent::TunnelRecv => TriggerEvent::TunnelRecv,
        _ => panic!("Unexpected event type: {:?}", s_event.event),
    };

    let outgoing_link_id = topology.routes[s_event.node_id][s_event.link_id].unwrap_or_else(|| {
        panic!("No outgoing link found for node {} with link index {}", s_event.node_id, s_event.link_id);
    });
    
    // Get immutable data first
    let to_node = linkstate.links[outgoing_link_id].to_node();
    // Calculate timing for propagation and transmission delay
    let current_duration = s_event.time.checked_duration_since(si.earliest_event_instant)
        .expect("s_event.time must not be earlier than sq.earliest_event_instant");

    let prop_us = if linkstate.links[outgoing_link_id].fixed_propagation() {
        linkstate.links[outgoing_link_id].get_prop_us_fixed()
    } else {
        let current_time_ms = current_duration.as_millis() as usize;
        linkstate.links[outgoing_link_id].get_prop_us_variable(current_time_ms)
    };
        
    // Now do the mutable borrow for sampling
    let transmission_delay = linkstate.links[outgoing_link_id].sample(current_duration);
    
    debug!("\tForwarding from node {} via link {} to node {}", 
           s_event.node_id, outgoing_link_id, to_node);
    
    let recv_s_event = SimulEvent {
        event: new_t_event,
        time: s_event.time + transmission_delay + prop_us,
        packet_id: s_event.packet_id,
        node_id: to_node,
        link_id: outgoing_link_id,
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
    pub coreside_out: usize,
}

impl ClientBasic {
    pub fn new(id: usize, coreside_out: usize) -> Self {
        Self {
            id,
            coreside_out,
        }
    }

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo,sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalSent => {
                make_network_receive_from_sent(s_event, topology, linkstate, si, sq);
            }
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_id].get_coreside_out_id();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(s_event, si, sq, outgoing_link, 0);
            }
            _ => {
                panic!("ClientBasic cannot handle s_event: {:?}", s_event.event);
            }
        }
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

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo,sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalRecv => {
                forward_network_receive_from_receive(s_event, topology, linkstate, si,sq);
            }
            TriggerEvent::TunnelRecv => {
                forward_network_receive_from_receive(s_event, topology, linkstate, si, sq);
            }
            TriggerEvent::PaddingSent { .. } | TriggerEvent::PaddingRecv => {
                // Relay handles padding traffic
            }
            _ => {
                panic!("RouterBasic cannot handle s_event: {:?}", s_event.event);
            }
        }
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

    pub fn handle_event(&self, s_event: &SimulEvent, topology: &NetworkTopology, linkstate: &mut NetworkLinkstate, si: &SimulInfo, sq: &mut SimulQueue) {
        match &s_event.event {
            TriggerEvent::NormalRecv => {
                let outgoing_link_id = topology.nodes[s_event.node_id].get_edgeside_out_id();
                let outgoing_link = &linkstate.links[outgoing_link_id];

                check_dependent_packets(s_event, si, sq, outgoing_link, 0);
            }
            TriggerEvent::NormalSent => {
                make_network_receive_from_sent(s_event, topology, linkstate, si, sq);
            }
            _ => {
                panic!("TrafficServerBasic cannot handle s_event: {:?}", s_event.event);
            }
        }
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn test_node_factory() {
        use std::collections::HashMap;
        use crate::topology_parse::create_node;
        
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