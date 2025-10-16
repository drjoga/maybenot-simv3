//! Integration tests for the settings module.

use maybenot_simulatorv3::settings::templates::{VpnSetting, vpn_links};
use rand::{SeedableRng, rngs::StdRng};

#[test]
fn test_vpn_setting_new() {
    // Test that we can create a VPN setting with default parameters
    let setting = VpnSetting::new();
    assert!(setting.is_ok(), "Failed to create VPN setting");
}

#[test]
fn test_vpn_setting_into_parts() {
    // Test that we can consume a setting and get topology and linkstate
    let setting = VpnSetting::new().unwrap();
    let (_topology, linkstate) = setting.into_parts();

    assert_eq!(linkstate.link_count(), 4, "VPN should have 4 links");
}

#[test]
fn test_vpn_setting_randomize_link() {
    // Test that we can randomize link parameters
    use maybenot_simulatorv3::links::LinkType;

    let mut setting = VpnSetting::new().unwrap();
    let mut rng = StdRng::seed_from_u64(42);

    // Get initial throughput
    let initial_tput = match setting
        .linkstate_mut()
        .get_link(vpn_links::CLIENT_UPSTREAM)
        .unwrap()
    {
        LinkType::FixedTput(link) => link.tput_bps,
        _ => panic!("Expected FixedTput link"),
    };

    // Randomize the client upstream link
    let result = setting.randomize_link(vpn_links::CLIENT_UPSTREAM, &mut rng);
    assert!(result.is_ok(), "Failed to randomize link");

    // Verify link was randomized
    let randomized_tput = match setting
        .linkstate_mut()
        .get_link(vpn_links::CLIENT_UPSTREAM)
        .unwrap()
    {
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
fn test_vpn_setting_randomize_all_links() {
    // Test randomizing all links in the VPN topology
    let mut setting = VpnSetting::new().unwrap();
    let mut rng = StdRng::seed_from_u64(123);

    // Randomize all 4 links
    for link_id in 0..4 {
        let result = setting.randomize_link(link_id, &mut rng);
        assert!(result.is_ok(), "Failed to randomize link {}", link_id);
    }

    // Verify we can still get the parts
    let (_topology, linkstate) = setting.into_parts();
    assert_eq!(linkstate.link_count(), 4);
}

#[test]
fn test_vpn_setting_invalid_link_id() {
    // Test that randomizing an invalid link ID returns an error
    let mut setting = VpnSetting::new().unwrap();
    let mut rng = StdRng::seed_from_u64(42);

    let result = setting.randomize_link(999, &mut rng);
    assert!(result.is_err(), "Should fail for invalid link ID");
    assert!(
        result.unwrap_err().contains("Link 999 not found"),
        "Error message should indicate link not found"
    );
}

#[test]
fn test_vpn_setting_clone() {
    // Test that we can clone a setting and randomize independently
    use maybenot_simulatorv3::links::LinkType;

    let base = VpnSetting::new().unwrap();
    let mut setting1 = base.clone();
    let mut setting2 = base.clone();

    let mut rng1 = StdRng::seed_from_u64(42);
    let mut rng2 = StdRng::seed_from_u64(99);

    // Randomize with different seeds
    setting1
        .randomize_link(vpn_links::CLIENT_UPSTREAM, &mut rng1)
        .unwrap();
    setting2
        .randomize_link(vpn_links::CLIENT_UPSTREAM, &mut rng2)
        .unwrap();

    // Get throughput from both settings
    let tput1 = match setting1
        .linkstate_mut()
        .get_link(vpn_links::CLIENT_UPSTREAM)
        .unwrap()
    {
        LinkType::FixedTput(link) => link.tput_bps,
        _ => panic!("Expected FixedTput link"),
    };
    let tput2 = match setting2
        .linkstate_mut()
        .get_link(vpn_links::CLIENT_UPSTREAM)
        .unwrap()
    {
        LinkType::FixedTput(link) => link.tput_bps,
        _ => panic!("Expected FixedTput link"),
    };

    // They should be different (different random seeds)
    assert_ne!(
        tput1, tput2,
        "Independently randomized settings should have different link parameters"
    );
}

#[test]
fn test_vpn_link_constants() {
    // Test that the link constants are correct
    assert_eq!(vpn_links::CLIENT_UPSTREAM, 0);
    assert_eq!(vpn_links::CLIENT_DOWNSTREAM, 1);
    assert_eq!(vpn_links::VPN_UPSTREAM, 2);
    assert_eq!(vpn_links::VPN_DOWNSTREAM, 3);
}

// 2-hop VPN Guard tests

#[test]
fn test_twohop_vpn_guard_new() {
    use maybenot_simulatorv3::settings::templates::TwoHopVpnGuardSetting;

    let setting = TwoHopVpnGuardSetting::new();
    assert!(setting.is_ok(), "Failed to create 2-hop VPN guard setting");
}

#[test]
fn test_twohop_vpn_guard_into_parts() {
    use maybenot_simulatorv3::settings::templates::TwoHopVpnGuardSetting;

    let setting = TwoHopVpnGuardSetting::new().unwrap();
    let (_topology, linkstate) = setting.into_parts();

    assert_eq!(linkstate.link_count(), 6, "2-hop VPN should have 6 links");
}

#[test]
fn test_twohop_vpn_guard_randomize() {
    use maybenot_simulatorv3::settings::templates::TwoHopVpnGuardSetting;

    let mut setting = TwoHopVpnGuardSetting::new().unwrap();
    let mut rng = StdRng::seed_from_u64(42);

    // Randomize all 6 links
    for link_id in 0..6 {
        let result = setting.randomize_link(link_id, &mut rng);
        assert!(result.is_ok(), "Failed to randomize link {}", link_id);
    }

    // Verify we can still get the parts
    let (_topology, linkstate) = setting.into_parts();
    assert_eq!(linkstate.link_count(), 6);
}

// 2-hop VPN Exit tests

#[test]
fn test_twohop_vpn_exit_new() {
    use maybenot_simulatorv3::settings::templates::TwoHopVpnExitSetting;

    let setting = TwoHopVpnExitSetting::new();
    assert!(setting.is_ok(), "Failed to create 2-hop VPN exit setting");
}

#[test]
fn test_twohop_vpn_exit_into_parts() {
    use maybenot_simulatorv3::settings::templates::TwoHopVpnExitSetting;

    let setting = TwoHopVpnExitSetting::new().unwrap();
    let (_topology, linkstate) = setting.into_parts();

    assert_eq!(linkstate.link_count(), 6, "2-hop VPN should have 6 links");
}

#[test]
fn test_twohop_vpn_exit_randomize() {
    use maybenot_simulatorv3::settings::templates::TwoHopVpnExitSetting;

    let mut setting = TwoHopVpnExitSetting::new().unwrap();
    let mut rng = StdRng::seed_from_u64(42);

    // Randomize all 6 links
    for link_id in 0..6 {
        let result = setting.randomize_link(link_id, &mut rng);
        assert!(result.is_ok(), "Failed to randomize link {}", link_id);
    }

    // Verify we can still get the parts
    let (_topology, linkstate) = setting.into_parts();
    assert_eq!(linkstate.link_count(), 6);
}

#[test]
fn test_twohop_vpn_link_constants() {
    use maybenot_simulatorv3::settings::templates::twohop_vpn_links;

    // Test that the link constants are correct
    assert_eq!(twohop_vpn_links::CLIENT_UPSTREAM, 0);
    assert_eq!(twohop_vpn_links::CLIENT_DOWNSTREAM, 1);
    assert_eq!(twohop_vpn_links::GUARD_UPSTREAM, 2);
    assert_eq!(twohop_vpn_links::GUARD_DOWNSTREAM, 3);
    assert_eq!(twohop_vpn_links::EXIT_UPSTREAM, 4);
    assert_eq!(twohop_vpn_links::EXIT_DOWNSTREAM, 5);
}
