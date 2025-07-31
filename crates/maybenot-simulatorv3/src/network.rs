use crate::links::{LinkType, create_link};
use crate::nodes::{NodeType, create_node};
use serde::Deserialize;
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


#[derive(Debug, Clone)]
pub struct NetworkLinkstate {
    pub links: Vec<LinkType>,
}

impl NetworkLinkstate {
    pub fn new() -> Self {
        Self {
            links: Vec::new(),
        }
    }

    pub fn add_link(&mut self, link: LinkType, id: usize) -> usize {
        assert_eq!(id, self.links.len(), "Link ID must match vector index");
        self.links.push(link);
        self.links.len() - 1
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

    pub fn link_count(&self) -> usize {
        self.links.len()
    }

    pub fn links(&self) -> &[LinkType] {
        &self.links
    }
}

#[derive(Debug, Clone)]
pub struct NetworkTopology {
    pub nodes: Vec<NodeType>,
    pub routes: Vec<Vec<Option<usize>>>, // routes[node_id][inlink] = Some(outlink) or None
    pub client: usize,
    pub traffic_server: usize,
    pub has_mb: bool,
    pub mb_client: usize,
    pub mb_server: usize,
}

impl NetworkTopology {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            routes: Vec::new(),
            client: 0,
            traffic_server: 0,
            has_mb: false,
            mb_client: 0,
            mb_server: 1,
        }
    }

    /// Load network configuration from a TOML file
    pub fn from_toml_file<P: AsRef<Path>>(path: P) -> Result<(Self, NetworkLinkstate), NetworkError> {
        let content = fs::read_to_string(path)
            .map_err(|e| NetworkError(format!("Failed to read file: {}", e)))?;
        
        Self::from_toml_str(&content)
    }

    /// Load network configuration from a TOML string
    pub fn from_toml_str(toml_str: &str) -> Result<(Self, NetworkLinkstate), NetworkError> {
        let config: NetworkConfig = toml::from_str(toml_str)
            .map_err(|e| NetworkError(format!("Failed to parse TOML: {}", e)))?;

        Self::from_config(config)
    }

    /// Create network from parsed configuration
    pub fn from_config(config: NetworkConfig) -> Result<(Self, NetworkLinkstate), NetworkError> {
        let mut topology = Self::new();
        let mut linkstate = NetworkLinkstate::new();

        // Find and validate client and traffic server nodes
        let mut client_id: Option<usize> = None;
        let mut traffic_server_id: Option<usize> = None;

        for node_config in &config.nodes {
            match node_config.node_type.as_str() {
                "ClientBasic" | "ClientMBN" => {
                    if client_id.is_some() {
                        return Err(NetworkError("Multiple Client nodes found. Only one is allowed.".to_string()));
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
        let client = client_id.ok_or_else(|| NetworkError("No Client node found. Exactly one is required.".to_string()))?;
        let traffic_server = traffic_server_id.ok_or_else(|| NetworkError("No TrafficServerBasic node found. Exactly one is required.".to_string()))?;

        // Set the client and traffic server IDs
        topology.client = client;
        topology.traffic_server = traffic_server;
        
        // Set MB fields (for future use)
        topology.has_mb = false;
        topology.mb_client = 0;
        topology.mb_server = 1;

        // Create nodes
        for node_config in &config.nodes {
            let node = create_node(&node_config.node_type, node_config.id, node_config.coreside_link, node_config.edgeside_link)
                .map_err(|e| NetworkError(format!("Failed to create node {}: {}", node_config.id, e)))?;
            topology.add_node(node, node_config.id);
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
            
            linkstate.add_link(link, link_config.id);
        }

        // Build routing matrix
        let num_nodes = config.nodes.len();
        let num_links = config.links.len();
        
        // Initialize routing matrix with None values
        topology.routes = vec![vec![None; num_links]; num_nodes];
        
        // Fill routing matrix from config
        for route_config in &config.routes {
            let node_id = route_config.node_id;
            if node_id >= num_nodes {
                return Err(NetworkError(format!("Invalid node_id {} in routes", node_id)));
            }
            
            for rule in &route_config.forwarding_rules {
                let in_link = rule.in_link;
                let out_link = rule.out_link;
                
                if in_link >= num_links {
                    return Err(NetworkError(format!("Invalid in_link {} for node {}", in_link, node_id)));
                }
                if out_link >= num_links {
                    return Err(NetworkError(format!("Invalid out_link {} for node {}", out_link, node_id)));
                }
                
                topology.routes[node_id][in_link] = Some(out_link);
            }
        }

        Ok((topology, linkstate))
    }

    /// Get outgoing link for a node given an incoming link
    pub fn get_outlink(&self, node_id: usize, in_link: usize) -> Option<usize> {
        *self.routes.get(node_id)?.get(in_link)?
    }

    pub fn add_node(&mut self, node: NodeType, id: usize) -> usize {
        assert_eq!(id, self.nodes.len(), "Node ID must match vector index");
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    pub fn get_node(&self, node_index: usize) -> Option<&NodeType> {
        self.nodes.get(node_index)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn nodes(&self) -> &[NodeType] {
        &self.nodes
    }

}

impl Default for NetworkTopology {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for NetworkLinkstate {
    fn default() -> Self {
        Self::new()
    }
}

// Legacy Network type for compatibility - use NetworkTopology + NetworkLinkstate instead
pub type Network = NetworkTopology;

