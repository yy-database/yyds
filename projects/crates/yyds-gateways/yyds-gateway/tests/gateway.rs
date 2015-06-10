use yyds_gateway::{GatewayConfig, GatewayKind, ALL_GATEWAY_KINDS, FORBIDDEN_LEGACY_SURFACES};

#[test]
fn all_gateway_kinds_have_distinct_ids() {
    let ids: Vec<_> = ALL_GATEWAY_KINDS.iter().map(|kind| kind.id()).collect();
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(ids.len(), unique.len());
}

#[test]
fn startup_banner_uses_crate_name_and_port() {
    let banner = yyds_gateway::startup_banner(&GatewayConfig::new(GatewayKind::Redis).with_port(6380));
    assert!(banner.contains("yyds-gateway-redis"));
    assert!(banner.contains("6380"));
}

#[test]
fn forbidden_legacy_surfaces_include_sql_gateway() {
    assert!(FORBIDDEN_LEGACY_SURFACES.contains(&"yyds-gateway/src/sql"));
    assert!(yyds_gateway::is_forbidden_legacy_surface("yyds-gateway/src/sql"));
}
