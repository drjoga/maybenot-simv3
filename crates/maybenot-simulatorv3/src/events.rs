use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Event {
    pub time: Duration,
    pub data: u32,
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for Event {}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.cmp(&self.time)
    }
}

pub struct EventQueue {
    heap: BinaryHeap<Event>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, event: Event) {
        self.heap.push(event);
    }

    pub fn pop(&mut self) -> Option<Event> {
        self.heap.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }
}

impl Default for EventQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_ordering() {
        let event1 = Event {
            time: Duration::from_millis(100),
            data: 1,
        };
        let event2 = Event {
            time: Duration::from_millis(50),
            data: 2,
        };

        assert!(event2 > event1);
    }

    #[test]
    fn test_event_queue() {
        let mut queue = EventQueue::new();

        let event1 = Event {
            time: Duration::from_millis(100),
            data: 1,
        };
        let event2 = Event {
            time: Duration::from_millis(50),
            data: 2,
        };

        queue.push(event1);
        queue.push(event2);

        let first = queue.pop().unwrap();
        assert_eq!(first.time, Duration::from_millis(50));

        let second = queue.pop().unwrap();
        assert_eq!(second.time, Duration::from_millis(100));
    }
}
