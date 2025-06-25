use crate::links::Link;
use crate::nodes::Node;

pub struct Network {
    nodes: Vec<Box<dyn Node>>,
    links: Vec<Link>,
}

impl Network {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            links: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node: Box<dyn Node>) -> usize {
        let node_index = self.nodes.len();
        self.nodes.push(node);
        node_index
    }

    pub fn add_link(&mut self, link: Link) -> usize {
        let link_index = self.links.len();
        self.links.push(link);
        link_index
    }

    pub fn get_node(&self, node_index: usize) -> Option<&dyn Node> {
        self.nodes.get(node_index).map(|n| n.as_ref())
    }

    pub fn get_node_mut(&mut self, node_index: usize) -> Option<&mut Box<dyn Node>> {
        self.nodes.get_mut(node_index)
    }

    pub fn get_node_by_id(&self, node_id: u32) -> Option<(usize, &dyn Node)> {
        self.nodes.iter().enumerate()
            .find(|(_, node)| node.node_id() == node_id)
            .map(|(idx, node)| (idx, node.as_ref()))
    }

    pub fn get_node_index_by_id(&self, node_id: u32) -> Option<usize> {
        self.nodes.iter().enumerate()
            .find(|(_, node)| node.node_id() == node_id)
            .map(|(idx, _)| idx)
    }

    pub fn get_link(&self, link_index: usize) -> Option<&Link> {
        self.links.get(link_index)
    }

    pub fn get_link_mut(&mut self, link_index: usize) -> Option<&mut Link> {
        self.links.get_mut(link_index)
    }

    pub fn find_link(&self, from_node_id: u32, to_node_id: u32) -> Option<(usize, &Link)> {
        self.links.iter().enumerate()
            .find(|(_, link)| link.from == from_node_id && link.to == to_node_id)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn link_count(&self) -> usize {
        self.links.len()
    }

    pub fn nodes(&self) -> &[Box<dyn Node>] {
        &self.nodes
    }

    pub fn links(&self) -> &[Link] {
        &self.links
    }
}

impl Default for Network {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::{ClientBasic, RelayBasic, TrafficServerBasic};
    use std::time::Duration;

    #[test]
    fn test_network_creation() {
        let network = Network::new();
        assert_eq!(network.node_count(), 0);
        assert_eq!(network.link_count(), 0);
    }

    #[test]
    fn test_add_node() {
        let mut network = Network::new();
        let node = Box::new(ClientBasic::new(1));
        let node_index = network.add_node(node);

        assert_eq!(node_index, 0);
        assert_eq!(network.node_count(), 1);
        assert!(network.get_node(0).is_some());
        assert_eq!(network.get_node(0).unwrap().node_id(), 1);
    }

    #[test]
    fn test_add_multiple_nodes() {
        let mut network = Network::new();
        
        let client = Box::new(ClientBasic::new(1));
        let relay = Box::new(RelayBasic::new(2));
        let server = Box::new(TrafficServerBasic::new(3));

        let client_idx = network.add_node(client);
        let relay_idx = network.add_node(relay);
        let server_idx = network.add_node(server);

        assert_eq!(client_idx, 0);
        assert_eq!(relay_idx, 1);
        assert_eq!(server_idx, 2);
        assert_eq!(network.node_count(), 3);
    }

    #[test]
    fn test_get_node_by_id() {
        let mut network = Network::new();
        
        let client = Box::new(ClientBasic::new(10));
        let relay = Box::new(RelayBasic::new(20));
        
        network.add_node(client);
        network.add_node(relay);

        let (idx, node) = network.get_node_by_id(20).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(node.node_id(), 20);

        let (idx, node) = network.get_node_by_id(10).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(node.node_id(), 10);

        assert!(network.get_node_by_id(99).is_none());

        // Test index lookup
        assert_eq!(network.get_node_index_by_id(20), Some(1));
        assert_eq!(network.get_node_index_by_id(10), Some(0));
        assert_eq!(network.get_node_index_by_id(99), None);
    }

    #[test]
    fn test_add_link() {
        let mut network = Network::new();
        
        let link = Link::new(1, 2, Duration::from_millis(10));
        let link_index = network.add_link(link);

        assert_eq!(link_index, 0);
        assert_eq!(network.link_count(), 1);
        
        let retrieved_link = network.get_link(0).unwrap();
        assert_eq!(retrieved_link.from, 1);
        assert_eq!(retrieved_link.to, 2);
        assert_eq!(retrieved_link.delay, Duration::from_millis(10));
    }

    #[test]
    fn test_find_link() {
        let mut network = Network::new();
        
        let link1 = Link::new(1, 2, Duration::from_millis(10));
        let link2 = Link::new(2, 3, Duration::from_millis(20));
        let link3 = Link::new(3, 1, Duration::from_millis(5));

        network.add_link(link1);
        network.add_link(link2);
        network.add_link(link3);

        let (idx, link) = network.find_link(2, 3).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(link.from, 2);
        assert_eq!(link.to, 3);

        assert!(network.find_link(99, 100).is_none());
    }

    #[test]
    fn test_network_iterators() {
        let mut network = Network::new();
        
        let client = Box::new(ClientBasic::new(1));
        let relay = Box::new(RelayBasic::new(2));
        
        network.add_node(client);
        network.add_node(relay);

        let link = Link::new(1, 2, Duration::from_millis(10));
        network.add_link(link);

        assert_eq!(network.nodes().len(), 2);
        assert_eq!(network.links().len(), 1);
        
        // Test that we can access all nodes
        for (i, node) in network.nodes().iter().enumerate() {
            assert!(node.node_id() == 1 || node.node_id() == 2);
            assert_eq!(i < 2, true);
        }
    }
}
