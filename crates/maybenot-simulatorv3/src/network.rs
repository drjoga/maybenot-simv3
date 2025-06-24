use crate::links::Link;
use crate::nodes::Node;
use std::collections::HashMap;
use std::{
    cmp::{max, Ordering},
    collections::{BinaryHeap, VecDeque},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};



pub struct Network {
    nodes: HashMap<u32, Node>,
    links: HashMap<(u32, u32), Link>,
}

impl Network {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            links: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.id, node);
    }

    pub fn add_link(&mut self, link: Link) {
        self.links.insert((link.from, link.to), link);
    }

    pub fn get_node(&self, node_id: u32) -> Option<&Node> {
        self.nodes.get(&node_id)
    }

    pub fn get_link(&self, from: u32, to: u32) -> Option<&Link> {
        self.links.get(&(from, to))
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn link_count(&self) -> usize {
        self.links.len()
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
        let node = Node::new(1);
        network.add_node(node);

        assert_eq!(network.node_count(), 1);
        assert!(network.get_node(1).is_some());
    }

    #[test]
    fn test_add_link() {
        let mut network = Network::new();
        let node1 = Node::new(1);
        let node2 = Node::new(2);

        network.add_node(node1);
        network.add_node(node2);

        let link = Link::new(1, 2, Duration::from_millis(10));
        network.add_link(link);

        assert_eq!(network.link_count(), 1);
        assert!(network.get_link(1, 2).is_some());
    }
}
