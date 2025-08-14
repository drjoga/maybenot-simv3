use crate::topology::{NetworkTopology, NetworkLinkstate};
use crate::nodes::NodeType;
use crate::links::LinkType;
use crate::mbn_nodes::{ClientMBN, RelayMBN, RelayMBNtserver};
use crate::linktrace::load_linktrace_from_file;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

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
    pub coreside_out: Option<usize>,
    pub edgeside_in: Option<usize>,
    pub edgeside_out: Option<usize>,
    #[serde(flatten)]
    pub params: HashMap<String, toml::Value>,
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

// Main parsing functions 

/// Load network configuration from a TOML file
pub fn load_topology_from_file<P: AsRef<Path>>(path: P) -> Result<(NetworkTopology, NetworkLinkstate), String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Network error: Failed to read file: {}", e))?;
    
    load_topology_from_str(&content)
}

/// Load network configuration from a TOML string
pub fn load_topology_from_str(toml_str: &str) -> Result<(NetworkTopology, NetworkLinkstate), String> {
    let config: NetworkConfig = toml::from_str(toml_str)
        .map_err(|e| format!("Network error: Failed to parse TOML: {}", e))?;

    build_topology_from_config(config)
}

/// Create network from parsed configuration
pub fn build_topology_from_config(config: NetworkConfig) -> Result<(NetworkTopology, NetworkLinkstate), String> {
    let mut topology = NetworkTopology::new();
    let mut linkstate = NetworkLinkstate::new();

    // Find and validate client and traffic server nodes
    let mut client_id: Option<usize> = None;
    let mut mb_server: Option<usize> = None;
    let mut traffic_server_id: Option<usize> = None;

    for node_config in &config.nodes {
        match node_config.node_type.as_str() {
            "ClientBasic" => {
                if client_id.is_some() {
                    return Err("Network error: Multiple Client nodes found. Only one is allowed.".to_string());
                }
                client_id = Some(node_config.id);
            }
            "ClientMBN" => {
                if client_id.is_some() {
                    return Err("Network error: Multiple Client nodes found. Only one is allowed.".to_string());
                }
                client_id = Some(node_config.id);
                topology.mb_client = node_config.id;
            }
            "RelayMBN" => {
                if mb_server.is_some() {
                    return Err("Network error: Multiple RelayMBN nodes found. Only one is allowed.".to_string());
                }
                mb_server = Some(node_config.id);
            }

            "TrafficServerBasic" => {
                if traffic_server_id.is_some() {
                    return Err("Network error: Multiple TrafficServerBasic nodes found. Only one is allowed.".to_string());
                }
                traffic_server_id = Some(node_config.id);
            }

            "RelayMBNtserver" => {
                if mb_server.is_some() {
                    return Err("Network error: Multiple MBN server nodes found. Only one is allowed.".to_string());
                }
                if traffic_server_id.is_some() {
                    return Err("Network error: Multiple traffic server nodes found. Only one is allowed.".to_string());
                }
                mb_server = Some(node_config.id);
                traffic_server_id = Some(node_config.id); // RelayMBNtserver acts as both
            }

            _ => {} // Other node types are fine
        }
    }

    // Ensure we have exactly one client and one traffic server
    let client = client_id.ok_or_else(|| "Network error: No Client node found. Exactly one is required.".to_string())?;
    let traffic_server = traffic_server_id.ok_or_else(|| "Network error: No traffic server node found. Exactly one TrafficServerBasic or RelayMBNtserver is required.".to_string())?;

    // Set the client and traffic server IDs
    topology.client = client;
    topology.traffic_server = traffic_server;
    
    // Set MB fields
    if let Some(mb_server_id) = mb_server {
        topology.has_mb = true;
        topology.mb_server = mb_server_id;
    }

    // Create nodes
    for node_config in &config.nodes {
        // Convert TOML values to strings for the factory function
        let params = convert_toml_params(&node_config.params);
        
        let node = create_node(
            &node_config.node_type,
            node_config.id,
            node_config.coreside_out,
            node_config.edgeside_in,
            node_config.edgeside_out,
            &params
        ).map_err(|e| format!("Network error: Failed to create node {}: {}", node_config.id, e))?;
        topology.add_node(node, node_config.id);
    }

    // Create links  
    for link_config in &config.links {
        // Convert TOML values to strings for the factory function
        let params = convert_toml_params(&link_config.params);

        let link = create_link(
            &link_config.link_type,
            link_config.id,
            link_config.from,
            link_config.to,
            &params
        ).map_err(|e| format!("Network error: Failed to create link {}: {}", link_config.id, e))?;
        
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
            return Err(format!("Network error: Invalid node_id {} in routes", node_id));
        }
        
        for rule in &route_config.forwarding_rules {
            let in_link = rule.in_link;
            let out_link = rule.out_link;
            
            if in_link >= num_links {
                return Err(format!("Network error: Invalid in_link {} for node {}", in_link, node_id));
            }
            if out_link >= num_links {
                return Err(format!("Network error: Invalid out_link {} for node {}", out_link, node_id));
            }
            
            topology.routes[node_id][in_link] = Some(out_link);
        }
    }

    Ok((topology, linkstate))
}

// Helper function to convert TOML values to strings
fn convert_toml_params(params: &HashMap<String, toml::Value>) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for (key, value) in params {
        let value_str = match value {
            toml::Value::String(s) => s.clone(),
            toml::Value::Integer(i) => i.to_string(),
            toml::Value::Float(f) => f.to_string(),
            toml::Value::Boolean(b) => b.to_string(),
            _ => continue, // Skip unsupported parameter types
        };
        result.insert(key.clone(), value_str);
    }
    result
}

// Factory functions 

// Factory function for creating nodes from TOML configuration 
pub fn create_node(
    node_type: &str,
    id: usize,
    coreside_out: Option<usize>,
    edgeside_in: Option<usize>,
    edgeside_out: Option<usize>,
    params: &HashMap<String, String>,
) -> Result<NodeType, String> {
    use crate::nodes::{ClientBasic, RouterBasic, TrafficServerBasic};
    
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
                        
            Ok(NodeType::ClientMBN(ClientMBN::new(
                id, coreside, Vec::new(),  
                0.0,0.0, false, None, None
            )))
        },
        "RelayMBN" => {
            let coreside_out_val = coreside_out
                .ok_or("RelayMBN requires coreside_out")?;
            let edgeside_in_val = edgeside_in
                .ok_or("RelayMBN requires edgeside_in")?;
            let edgeside_out_val = edgeside_out
                .ok_or("RelayMBN requires edgeside_out")?;
            
            Ok(NodeType::RelayMBN(RelayMBN::new(
                id, coreside_out_val, edgeside_in_val, edgeside_out_val, Vec::new(), 
                0.0, 0.0, false, None, None
            )))
        },
        "RelayMBNtserver" => {
            // RelayMBNtserver uses edgeside_in and edgeside_out
            let edgeside_out_val = edgeside_out
                .ok_or("RelayMBNtserver requires edgeside_out")?;
            let edgeside_in_val = edgeside_in
                .ok_or("RelayMBNtserver requires edgeside_in")?;
                           
            // Parse ts_prop_us parameter specific to RelayMBNtserver
            let ts_prop_us = params
                .get("ts_prop_us")
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_micros)
                .unwrap_or(Duration::from_micros(0)); // Default to 0us if not specified
            
            Ok(NodeType::RelayMBNtserver(RelayMBNtserver::new(
                id, edgeside_in_val, edgeside_out_val, Vec::new(),
                0.0, 0.0, false, None, None, ts_prop_us
            )))
        },
        _ => Err(format!("Unknown node type: {}", node_type)),
    }
}

// Factory function for creating links from TOML configuration 
pub fn create_link(
    link_type: &str,
    id: usize,
    from: usize,
    to: usize,
    params: &HashMap<String, String>,
) -> Result<LinkType, String> {
    use crate::links::{FixedTputLink, HiTraceTputLink, StdTraceTputLink};
    
    // Parse prop_us parameter (required for all link types)
    let prop_us = params
        .get("prop_us")
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_micros)
        .unwrap_or(Duration::from_micros(0)); // Default to 0us if not specified

    match link_type {
        "FixedTput" => {
            // Simplex link - requires tput_bps parameter
            let tput = params
                .get("tput_bps")
                .ok_or("FixedTput requires tput_bps parameter")?
                .parse::<u64>()
                .map_err(|_| "Invalid tput_bps value - must be a valid u64")?;
            
            Ok(LinkType::FixedTput(FixedTputLink::new(id, from, to, prop_us, tput)))
        }
        "HiTraceTput" => {
            let trace_file = params
                .get("trace_file")
                .ok_or("HiTraceTput requires trace_file parameter")?;
            
            let linktrace = load_linktrace_from_file(trace_file)
                .map_err(|e| format!("Failed to load trace file '{}': {}", trace_file, e))?;
            
            Ok(LinkType::HiTraceTput(HiTraceTputLink::new(id, from, to, prop_us, linktrace)))
        }
        "StdTraceTput" => {
            let trace_file = params
                .get("trace_file")
                .ok_or("StdTraceTput requires trace_file parameter")?;
            
            let linktrace = load_linktrace_from_file(trace_file)
                .map_err(|e| format!("Failed to load trace file '{}': {}", trace_file, e))?;
            
            Ok(LinkType::StdTraceTput(StdTraceTputLink::new(id, from, to, prop_us, linktrace)))
        }
        _ => Err(format!("Unknown link type: {}", link_type)),
    }
}

/// Modify TOML configuration by applying parameter changes specified in modifier string.
/// 
/// # Arguments
/// * `toml_in` - Input TOML configuration string
/// * `modifier_string` - Modifications in format: "SectionType:ID::param1:value1::param2:value2\n..."
///                      Supported SectionTypes: "Node", "Link"
/// 
/// # Example
/// ```
/// use maybenot_simulatorv3::modify_toml;
/// 
/// let original_toml = r#"
/// [[Link]]
/// id = 0
/// prop_us = 10000
/// tput_bps = 100000000
/// "#;
/// let modifications = "Link:0::prop_us:5000::tput_bps:50000000";
/// let modified_toml = modify_toml(&original_toml, modifications).unwrap();
/// ```
pub fn modify_toml(toml_in: &str, modifier_string: &str) -> Result<String, String> {
    // Parse input TOML into a mutable value
    let mut toml_value: toml::Value = toml::from_str(toml_in)
        .map_err(|e| format!("Failed to parse input TOML: {}", e))?;
    
    // Get the root table
    let root_table = toml_value.as_table_mut()
        .ok_or("TOML root is not a table")?;
    
    // Process each modification line
    for line in modifier_string.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        
        // Parse line format: "SectionType:ID::param1:value1::param2:value2"
        let parts: Vec<&str> = line.split("::").collect();
        if parts.is_empty() {
            return Err("Empty modification line".to_string());
        }
        
        // Parse section type and ID from first part
        let section_parts: Vec<&str> = parts[0].split(':').collect();
        if section_parts.len() != 2 {
            return Err(format!("Invalid section format in line: {}", line));
        }
        
        let section_type = section_parts[0];
        let section_id: usize = section_parts[1].parse()
            .map_err(|_| format!("Invalid section ID in line: {}", line))?;
        
        // Find the appropriate section array
        let section_array = match section_type {
            "Node" => root_table.get_mut("Node"),
            "Link" => root_table.get_mut("Link"),
            _ => return Err(format!("Unsupported section type: {}", section_type)),
        };
        
        let section_array = section_array
            .and_then(|v| v.as_array_mut())
            .ok_or(format!("Section {} is not an array", section_type))?;
        
        // Find the specific section by ID
        let target_section = section_array.iter_mut()
            .find(|entry| {
                entry.as_table()
                    .and_then(|table| table.get("id"))
                    .and_then(|id| id.as_integer())
                    .map(|id| id == section_id as i64)
                    .unwrap_or(false)
            })
            .ok_or(format!("Section {} with ID {} not found", section_type, section_id))?;
        
        let target_table = target_section.as_table_mut()
            .ok_or("Section entry is not a table".to_string())?;
        
        // Apply parameter modifications from remaining parts
        for param_part in &parts[1..] {
            let param_kv: Vec<&str> = param_part.split(':').collect();
            if param_kv.len() != 2 {
                return Err(format!("Invalid parameter format in: {}", param_part));
            }
            
            let param_name = param_kv[0];
            let param_value_str = param_kv[1];
            
            // Convert value to appropriate TOML type
            let param_value = if let Ok(int_val) = param_value_str.parse::<i64>() {
                toml::Value::Integer(int_val)
            } else if let Ok(float_val) = param_value_str.parse::<f64>() {
                toml::Value::Float(float_val)
            } else if let Ok(bool_val) = param_value_str.parse::<bool>() {
                toml::Value::Boolean(bool_val)
            } else {
                toml::Value::String(param_value_str.to_string())
            };
            
            // Update the parameter in the target section
            target_table.insert(param_name.to_string(), param_value);
        }
    }
    
    // Serialize back to TOML string
    toml::to_string_pretty(&toml_value)
        .map_err(|e| format!("Failed to serialize TOML: {}", e))
}

// Modifies the prop_us parameter for all Link instances in a TOML string.
pub fn set_toml_propagation_us(toml_in: &str, delay_us: u64) -> String {
    // Parse input TOML into a mutable value
    let mut toml_value: toml::Value = toml::from_str(toml_in)
        .unwrap_or_else(|e| panic!("Failed to parse input TOML: {}", e));
    
    // Get the root table
    let root_table = toml_value.as_table_mut()
        .unwrap_or_else(|| panic!("TOML root is not a table"));
    
    // Find the Link section array
    let link_array = root_table.get_mut("Link")
        .and_then(|v| v.as_array_mut())
        .unwrap_or_else(|| panic!("Link section is not an array or doesn't exist"));
    
    // Update prop_us for all Link instances
    for link_entry in link_array.iter_mut() {
        let link_table = link_entry.as_table_mut()
            .unwrap_or_else(|| panic!("Link entry is not a table"));
        
        // Set the prop_us parameter to the specified value
        link_table.insert("prop_us".to_string(), toml::Value::Integer(delay_us as i64));
    }
    
    // Serialize back to TOML string
    toml::to_string_pretty(&toml_value)
        .unwrap_or_else(|e| panic!("Failed to serialize TOML: {}", e))
}