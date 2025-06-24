pub struct Node {
    pub id: u32,
}

impl Node {
    pub fn new(id: u32) -> Self {
        Self { id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node = Node::new(42);
        assert_eq!(node.id, 42);
    }
}
