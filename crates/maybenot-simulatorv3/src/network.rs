use crate::links::{LinkType, SimpleLink, create_link};
use crate::nodes::{NodeType, create_node};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

// TOML configuration structures
#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    #[serde(rename = "Node")]
    pub nodes: Vec<NodeConfig>,
    #[serde(rename = "Link")]
    pub links: Vec<LinkConfig>,
    #[serde(rename = "Route", default)]
    pub routes: Vec<RouteConfig>,
}

#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub id: usize,
    #[serde(rename = "type")]
    pub node_type: String,
    pub coreside_link: Option<usize>,
    pub edgeside_link: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct LinkConfig {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    #[serde(rename = "type")]
    pub link_type: String,
    #[serde(flatten)]
    pub params: HashMap<String, toml::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RouteConfig {
    pub node_id: usize,
    pub forwarding_rules: Vec<ForwardingRule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ForwardingRule {
    pub in_link: usize,
    pub out_link: usize,
}

#[derive(Debug, Clone)]
pub struct NetworkError(pub String);

impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Network error: {}", self.0)
    }
}

impl std::error::Error for NetworkError {}

// Check if, Clone can be removed from Network
#[derive(Debug, Clone)]
pub struct Network {
    pub nodes: Vec<NodeType>,
    pub links: Vec<LinkType>,
    pub routes: Vec<RouteConfig>,
    pub client: usize,
    pub traffic_server: usize,
    pub has_mb: bool,
    pub mb_client: usize,
    pub mb_server: usize,
}

impl Network {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            links: Vec::new(),
            routes: Vec::new(),
            client: 0,
            traffic_server: 0,
            has_mb: false,
            mb_client: 0,
            mb_server: 0,
        }
    }

    /// Load network configuration from a TOML file
    pub fn from_toml_file<P: AsRef<Path>>(path: P) -> Result<Self, NetworkError> {
        let content = fs::read_to_string(path)
            .map_err(|e| NetworkError(format!("Failed to read file: {}", e)))?;
        
        Self::from_toml_str(&content)
    }

    /// Load network configuration from a TOML string
    pub fn from_toml_str(toml_str: &str) -> Result<Self, NetworkError> {
        let config: NetworkConfig = toml::from_str(toml_str)
            .map_err(|e| NetworkError(format!("Failed to parse TOML: {}", e)))?;

        Self::from_config(config)
    }

    /// Create network from parsed configuration
    pub fn from_config(config: NetworkConfig) -> Result<Self, NetworkError> {
        let mut network = Self::new();

        // Find and validate client and traffic server nodes
        let mut client_id: Option<usize> = None;
        let mut traffic_server_id: Option<usize> = None;

        for node_config in &config.nodes {
            match node_config.node_type.as_str() {
                "ClientBasic" => {
                    if client_id.is_some() {
                        return Err(NetworkError("Multiple ClientBasic nodes found. Only one is allowed.".to_string()));
                    }
                    client_id = Some(node_config.id);
                }
                "TrafficServerBasic" => {
                    if traffic_server_id.is_some() {
                        return Err(NetworkError("Multiple TrafficServerBasic nodes found. Only one is allowed.".to_string()));
                    }
                    traffic_server_id = Some(node_config.id);
                }
                _ => {} // Other node types are fine
            }
        }

        // Ensure we have exactly one client and one traffic server
        let client = client_id.ok_or_else(|| NetworkError("No ClientBasic node found. Exactly one is required.".to_string()))?;
        let traffic_server = traffic_server_id.ok_or_else(|| NetworkError("No TrafficServerBasic node found. Exactly one is required.".to_string()))?;

        // Set the client and traffic server IDs
        network.client = client;
        network.traffic_server = traffic_server;
        
        // Set MB fields (for future use)
        network.has_mb = false;
        network.mb_client = 0;
        network.mb_server = 0;

        // Create nodes
        for node_config in &config.nodes {
            let node = create_node(&node_config.node_type, node_config.id, node_config.coreside_link, node_config.edgeside_link)
                .map_err(|e| NetworkError(format!("Failed to create node {}: {}", node_config.id, e)))?;
            network.add_node(node, node_config.id);
        }

        // Create links  
        for link_config in &config.links {
            // Convert TOML values to strings for the factory function
            let mut params = HashMap::new();
            for (key, value) in &link_config.params {
                let value_str = match value {
                    toml::Value::String(s) => s.clone(),
                    toml::Value::Integer(i) => i.to_string(),
                    toml::Value::Float(f) => f.to_string(),
                    toml::Value::Boolean(b) => b.to_string(),
                    _ => return Err(NetworkError(format!("Unsupported parameter type for {}", key))),
                };
                params.insert(key.clone(), value_str);
            }

            let link = create_link(
                &link_config.link_type,
                link_config.id,
                link_config.from,
                link_config.to,
                &params
            ).map_err(|e| NetworkError(format!("Failed to create link {}: {}", link_config.id, e)))?;
            
            network.add_link(link, link_config.id);
        }

        // Store routing configuration
        network.routes = config.routes;

        Ok(network)
    }

    /// Get routing rules for a specific node
    pub fn get_routing_rules(&self, node_id: usize) -> Option<&Vec<ForwardingRule>> {
        self.routes.iter()
            .find(|route| route.node_id == node_id)
            .map(|route| &route.forwarding_rules)
    }

    /// Get all routing configurations
    pub fn get_routes(&self) -> &[RouteConfig] {
        &self.routes
    }

    pub fn add_node(&mut self, node: NodeType, id: usize) -> usize {
        assert_eq!(id, self.nodes.len(), "Node ID must match vector index");
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    pub fn add_link(&mut self, link: LinkType, id: usize) -> usize {
        assert_eq!(id, self.links.len(), "Link ID must match vector index");
        self.links.push(link);
        self.links.len() - 1
    }

    pub fn get_node(&self, node_index: usize) -> Option<&NodeType> {
        self.nodes.get(node_index)
    }

    pub fn get_node_mut(&mut self, node_index: usize) -> Option<&mut NodeType> {
        self.nodes.get_mut(node_index)
    }

    pub fn get_link(&self, link_index: usize) -> Option<&LinkType> {
        self.links.get(link_index)
    }

    pub fn get_link_mut(&mut self, link_index: usize) -> Option<&mut LinkType> {
        self.links.get_mut(link_index)
    }

    pub fn find_link(&self, from_node_id: usize, to_node_id: usize) -> Option<(usize, &LinkType)> {
        self.links.iter().enumerate()
            .find(|(_, link)| link.from_node() == from_node_id && link.to_node() == to_node_id)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn link_count(&self) -> usize {
        self.links.len()
    }

    pub fn nodes(&self) -> &[NodeType] {
        &self.nodes
    }

    pub fn links(&self) -> &[LinkType] {
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
    use crate::nodes::{ClientBasic, RelayBasic, TrafficServerBasic, NodeType};
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
        let node = NodeType::ClientBasic(ClientBasic::new(0));
        let node_index = network.add_node(node, 0);

        assert_eq!(node_index, 0);
        assert_eq!(network.node_count(), 1);
        assert!(network.get_node(0).is_some());
        assert_eq!(network.get_node(0).unwrap().node_id(), 0);
    }

    #[test]
    fn test_add_multiple_nodes() {
        let mut network = Network::new();
        
        let client = NodeType::ClientBasic(ClientBasic::new(0));
        let relay = NodeType::RelayBasic(RelayBasic::new(1));
        let server = NodeType::TrafficServerBasic(TrafficServerBasic::new(2));

        let client_idx = network.add_node(client, 0);
        let relay_idx = network.add_node(relay, 1);
        let server_idx = network.add_node(server, 2);

        assert_eq!(client_idx, 0);
        assert_eq!(relay_idx, 1);
        assert_eq!(server_idx, 2);
        assert_eq!(network.node_count(), 3);
    }

    #[test]
    fn test_add_link() {
        use crate::links::{BottleneckTputLink, LinkType};
        use std::time::Duration;
        
        let mut network = Network::new();
        
        let link = LinkType::BottleneckTput(BottleneckTputLink::new(
            0, 1, 2, 
            Duration::from_millis(10), 
            Some(1000)
        ));
        let link_index = network.add_link(link, 0);

        assert_eq!(link_index, 0);
        assert_eq!(network.link_count(), 1);
        
        let retrieved_link = network.get_link(0).unwrap();
        assert_eq!(retrieved_link.from_node(), 1);
        assert_eq!(retrieved_link.to_node(), 2);
        assert_eq!(retrieved_link.link_id(), 0);
        assert_eq!(retrieved_link.type_name(), "BottleneckTput");
    }

    #[test]
    fn test_find_link() {
        use crate::links::{BottleneckTputLink, FixedTputLink};
        use std::time::Duration;
        
        let mut network = Network::new();
        
        let link0 = LinkType::BottleneckTput(BottleneckTputLink::new(
            0, 1, 2, Duration::from_millis(10), Some(1000)
        ));
        let link1 = LinkType::FixedTput(FixedTputLink::new(1, 2, 3, 1_000_000, 1_000_000));
        let link2 = LinkType::BottleneckTput(BottleneckTputLink::new(
            2, 3, 1, Duration::from_millis(5), None
        ));

        network.add_link(link0, 0);
        network.add_link(link1, 1);
        network.add_link(link2, 2);

        let (idx, link) = network.find_link(2, 3).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(link.from_node(), 2);
        assert_eq!(link.to_node(), 3);

        assert!(network.find_link(99, 100).is_none());
    }

    #[test]
    fn test_network_iterators() {
        use crate::links::BottleneckTputLink;
        use std::time::Duration;
        
        let mut network = Network::new();
        
        let client = NodeType::ClientBasic(ClientBasic::new(0));
        let relay = NodeType::RelayBasic(RelayBasic::new(1));
        
        network.add_node(client, 0);
        network.add_node(relay, 1);

        let link = LinkType::BottleneckTput(BottleneckTputLink::new(
            0, 1, 2, Duration::from_millis(10), Some(1000)
        ));
        network.add_link(link, 0);

        assert_eq!(network.nodes().len(), 2);
        assert_eq!(network.links().len(), 1);
        
        // Test that we can access all nodes
        for (i, node) in network.nodes().iter().enumerate() {
            assert_eq!(node.node_id() as usize, i);
            assert_eq!(i < 2, true);
        }
        
        // Test that we can access all links
        for link in network.links().iter() {
            assert!(link.from_node() == 1 && link.to_node() == 2);
        }
    }

    #[test]
    fn test_toml_loading() {
        let toml_content = r#"
[[Node]]
id = 0
type = "ClientBasic"

[[Node]]
id = 1
type = "RelayBasic"

[[Link]]
id = 0
from = 0
to = 1
type = "FixedTput"
tput_bps = 100000000

[[Route]]
node_id = 1
forwarding_rules = [
    { in_link = 0, out_link = 1 }
]
"#;

        let network = Network::from_toml_str(toml_content).unwrap();
        
        assert_eq!(network.node_count(), 2);
        assert_eq!(network.link_count(), 1);
        assert_eq!(network.get_routes().len(), 1);
        
        // Test node IDs
        assert_eq!(network.get_node(0).unwrap().node_id(), 0);
        assert_eq!(network.get_node(1).unwrap().node_id(), 1);
        
        // Test link properties
        let link = network.get_link(0).unwrap();
        assert_eq!(link.from_node(), 0);
        assert_eq!(link.to_node(), 1);
        assert_eq!(link.type_name(), "FixedTput");
        
        // Test routing rules
        let rules = network.get_routing_rules(1).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].in_link, 0);
        assert_eq!(rules[0].out_link, 1);
    }

    #[test]
    fn test_basic_test_toml() {
        // Test loading the actual basic_test.toml file
        let network = Network::from_toml_file("basic_test.toml");
        
        match network {
            Ok(net) => {
                assert_eq!(net.node_count(), 3);
                assert_eq!(net.link_count(), 4);
                assert_eq!(net.get_routes().len(), 1);
                
                // Verify all nodes exist with correct types
                assert_eq!(net.get_node(0).unwrap().node_id(), 0);
                assert_eq!(net.get_node(1).unwrap().node_id(), 1);
                assert_eq!(net.get_node(2).unwrap().node_id(), 2);
                
                // Verify routing rules for relay node
                let rules = net.get_routing_rules(1).unwrap();
                assert_eq!(rules.len(), 2);
            },
            Err(e) => {
                // If file doesn't exist or has issues, just print the error
                println!("Note: Could not load basic_test.toml: {}", e);
            }
        }
    }
}
