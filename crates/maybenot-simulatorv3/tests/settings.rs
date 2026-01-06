//! Integration tests for the settings module.

use maybenot_simulatorv3::links::LinkType;
use maybenot_simulatorv3::settings::Setting;
use rand::{SeedableRng, rngs::StdRng};

#[test]
fn test_vpn_create() {
    // Test that we can create a VPN topology and linkstate
    let (_topology, linkstate) = Setting::Vpn.create().unwrap();
    assert_eq!(linkstate.link_count(), 4, "VPN should have 4 links");
}

#[test]
fn test_vpn_linkstate_randomize() {
    // Test that we can randomize link parameters
    let (_topology, linkstate) = Setting::Vpn.create().unwrap();
    let mut rng = StdRng::seed_from_u64(42);

    // Get initial throughput (link 0 = client upstream)
    let initial_tput = match linkstate.get_link(0).unwrap() {
        LinkType::FixedTput(link) => link.tput_bps,
        _ => panic!("Expected FixedTput link"),
    };

    // Clone and randomize (±20% variation)
    let randomized = linkstate.clone_randomized(&mut rng, 0.2);

    // Verify link was randomized
    let randomized_tput = match randomized.get_link(0).unwrap() {
        LinkType::FixedTput(link) => link.tput_bps,
        _ => panic!("Expected FixedTput link"),
    };

    // Check that throughput changed
    assert_ne!(
        initial_tput, randomized_tput,
        "Link throughput should have changed"
    );

    // Check that throughput is within ±20% range
    let expected_min = (initial_tput as f64 * 0.8) as u64;
    let expected_max = (initial_tput as f64 * 1.2) as u64;
    assert!(
        randomized_tput >= expected_min && randomized_tput <= expected_max,
        "Randomized throughput {} should be within ±20% of {} ({} to {})",
        randomized_tput,
        initial_tput,
        expected_min,
        expected_max
    );
}

#[test]
fn test_vpn_linkstate_randomize_all_links() {
    // Test randomizing all links via clone_randomized
    let (_topology, linkstate) = Setting::Vpn.create().unwrap();
    let mut rng = StdRng::seed_from_u64(123);

    // Clone and randomize all links at once (±20% variation)
    let randomized = linkstate.clone_randomized(&mut rng, 0.2);

    // Verify we still have 4 links
    assert_eq!(randomized.link_count(), 4);
}

#[test]
fn test_vpn_linkstate_independent_randomization() {
    // Test that independent randomizations produce different results
    let (_topology, linkstate) = Setting::Vpn.create().unwrap();

    let mut rng1 = StdRng::seed_from_u64(42);
    let mut rng2 = StdRng::seed_from_u64(99);

    // Randomize with different seeds (±20% variation)
    let linkstate1 = linkstate.clone_randomized(&mut rng1, 0.2);
    let linkstate2 = linkstate.clone_randomized(&mut rng2, 0.2);

    // Get throughput from both linkstates (link 0 = client upstream)
    let tput1 = match linkstate1.get_link(0).unwrap() {
        LinkType::FixedTput(link) => link.tput_bps,
        _ => panic!("Expected FixedTput link"),
    };
    let tput2 = match linkstate2.get_link(0).unwrap() {
        LinkType::FixedTput(link) => link.tput_bps,
        _ => panic!("Expected FixedTput link"),
    };

    // They should be different (different random seeds)
    assert_ne!(
        tput1, tput2,
        "Independently randomized linkstates should have different link parameters"
    );
}

// Multi-hop Guard tests

#[test]
fn test_multihop_guard_create() {
    let (_topology, linkstate) = Setting::MultihopGuard.create().unwrap();
    assert_eq!(linkstate.link_count(), 6, "Multi-hop should have 6 links");
}

#[test]
fn test_multihop_guard_randomize() {
    let (_topology, linkstate) = Setting::MultihopGuard.create().unwrap();
    let mut rng = StdRng::seed_from_u64(42);

    // Clone and randomize all 6 links (±20% variation)
    let randomized = linkstate.clone_randomized(&mut rng, 0.2);

    // Verify we still have 6 links
    assert_eq!(randomized.link_count(), 6);
}

// Multi-hop Exit tests

#[test]
fn test_multihop_exit_create() {
    let (_topology, linkstate) = Setting::MultihopExit.create().unwrap();
    assert_eq!(linkstate.link_count(), 6, "Multi-hop should have 6 links");
}

#[test]
fn test_multihop_exit_randomize() {
    let (_topology, linkstate) = Setting::MultihopExit.create().unwrap();
    let mut rng = StdRng::seed_from_u64(42);

    // Clone and randomize all 6 links (±20% variation)
    let randomized = linkstate.clone_randomized(&mut rng, 0.2);

    // Verify we still have 6 links
    assert_eq!(randomized.link_count(), 6);
}
