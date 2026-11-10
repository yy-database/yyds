/**
 * `@yydb/yyds/node` — Node N-API facade.
 *
 * Browser hosts are not supported in this package yet.
 */

export type { YydsBindings, NodeStatusBinding, ServiceBinding } from "../bindings.ts";
export type { YydsProtocol } from "./cluster.ts";
export { launchYydsServer, yydsStartArguments } from "./cluster.ts";
export { isYydsNativeInstalled, loadYydsNative } from "./load.ts";
export type { StartNodeOptions } from "./runtime.ts";
export { parseStartOptions, runNodeStart } from "./runtime.ts";

export { YydsError, configurationError, nativeError } from "../errors.ts";
export type { YydsErrorDetails, YydsErrorPhase, YydsExecution, YydsRetry } from "../errors.ts";
export type { LocalStorageOptions, PoolOptions, TcpEndpoint, TlsIdentityOptions, TlsOptions } from "../types.ts";
export { Yyds, YydsListener, YYDS_SURFACE_GAPS } from "../yyds.ts";
export type {
    YydsCapabilities,
    YydsClusterConfig,
    YydsListenOptions,
    YydsOptions,
    YydsPeerConfig,
    YydsState,
} from "../yyds.ts";
export { YydsClient } from "../client.ts";
export type {
    YydsAuth,
    YydsClientCapabilities,
    YydsClientOptions,
    YydsClientState,
    YydsDiscoveryOptions,
    YydsVosDescriptor,
} from "../client.ts";
