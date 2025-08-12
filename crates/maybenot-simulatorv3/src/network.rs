use crate::links::LinkType;
use crate::nodes::NodeType;
use crate::mbn_nodes::MBNNode;


#[derive(Debug, Clone)]
pub struct NetworkLinkstate {
    pub links: Vec<LinkType>,
}

impl Default for NetworkLinkstate {
    fn default() -> Self {
        Self::new()
    }
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

    pub fn link_count(&self) -> usize {
        self.links.len()
    }

    pub fn links(&self) -> &[LinkType] {
        &self.links
    }
}

#[derive(Debug)]
pub struct NetworkTopology {
    pub nodes: Vec<NodeType>,
    pub routes: Vec<Vec<Option<usize>>>, // routes[node_id][inlink] = Some(outlink) or None
    pub client: usize,
    pub traffic_server: usize,
    pub has_mb: bool,
    pub mb_client: usize,
    pub mb_server: usize,
}

impl Default for NetworkTopology {
    fn default() -> Self {
        Self::new()
    }
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
            mb_server: 0,
        }
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

    pub fn get_mbn_client(&self) -> &dyn MBNNode {
        match &self.nodes[self.mb_client] {
            NodeType::ClientMBN(client) => client,
            _ => panic!("MBN client node not found or wrong type"),
        }
    }

    pub fn get_mbn_server(&self) -> &dyn MBNNode {
        match &self.nodes[self.mb_server] {
            NodeType::RelayMBN(server) => server,
            NodeType::RelayMBNtserver(server) => server,
            _ => panic!("MBN server node not found or wrong type"),
        }
    }

}
