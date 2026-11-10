/**
 * `Yyds` — the durable node runtime owner.
 *
 * The constructor only stores and validates configuration. `open()` completes node
 * identity and storage initialization through the existing native primitives
 * (`nodeInit`, `NodeLeaseHandle`, `LocalExecutorHandle`, `ExpirySweeperHandle`),
 * `listen()` starts the loopback application port, and `close()` cascades over the
 * resources this object owns. All cluster, replication, routing and protocol
 * semantics stay in the native layer; this module adds no second implementation.
 */

import type { ExpirySweeperBinding, LocalExecutorBinding, ServiceBinding, YydsBindings } from "./bindings.ts";
import { configurationError, nativeError, YydsError } from "./errors.ts";
import { isYydsNativeInstalled, loadYydsNative } from "./node/load.ts";
import {
    assertEndpoint,
    assertEndpoints,
    assertPositiveInteger,
    assertTls,
    isRecord,
    type LocalStorageOptions,
    type TcpEndpoint,
    type TlsOptions,
} from "./types.ts";

const DEFAULT_EXPIRY_SWEEP_INTERVAL_MS = 100;
const LOOPBACK_HOST = "127.0.0.1";

export type { LocalStorageOptions, TcpEndpoint, TlsOptions };

export type YydsClusterConfig =
    | { readonly mode: "bootstrap"; readonly clusterId: string }
    | { readonly mode: "join"; readonly seeds: readonly TcpEndpoint[] };

export interface YydsPeerConfig {
    readonly endpoint: TcpEndpoint;
    readonly tls: TlsOptions;
}

export interface YydsOptions {
    readonly nodeId: string;
    readonly storage: LocalStorageOptions;
    readonly cluster: YydsClusterConfig;
    readonly peer: YydsPeerConfig;
    /** Background expiry reclamation period in milliseconds. Defaults to 100. */
    readonly expirySweepIntervalMs?: number;
}

export interface YydsListenOptions {
    /** Loopback application port. `port: 0` asks the native server for an ephemeral one. */
    readonly endpoint?: TcpEndpoint;
    /** Rejected while the native wire server only reads bootstrap tokens from `YYDS_AUTH_TOKENS`. */
    readonly authentication?: { readonly token: string };
}

export type YydsState = "new" | "opening" | "open" | "closing" | "closed" | "failed";

export interface YydsCapabilities {
    readonly product: "yyds";
    readonly version: string | null;
    readonly identity: { readonly nodeId: string; readonly clusterId: string | null; readonly mode: "bootstrap" | "join" };
    readonly lifecycle: { readonly state: YydsState };
    readonly storage: { readonly directory: string; readonly persistent: true };
    readonly cluster: {
        readonly qualified: boolean;
        readonly members: number;
        readonly replication: boolean;
        readonly failover: boolean;
        readonly reconfiguration: boolean;
        readonly note: string;
    };
    readonly peer: { readonly transport: boolean; readonly tls: boolean; readonly note: string };
    readonly application: { readonly wireProtocol: boolean; readonly authentication: string };
    readonly gaps: readonly string[];
}

/** Honest list of primitives this build does not export yet. */
export const YYDS_SURFACE_GAPS: readonly string[] = [
    "cluster.join: no native join primitive; a bootstrap node stays single-member and is not cluster-qualified",
    "peer.transport: peer endpoint and tls are validated at construction, but no native peer transport is exported",
    "client.transport: no native YY wire client primitive for YydsClient",
    "client.dataPlane: VOS table/prepare/transaction are not carried by the wire protocol yet",
    "listen.authentication: wire bootstrap tokens are only read from the YYDS_AUTH_TOKENS environment",
    "lease.release: node lease and executor are reclaimed with handle garbage collection; no explicit release primitive",
];

function validateOptions(options: unknown): void {
    if (!isRecord(options)) throw configurationError("Yyds options must be an object");
    if (typeof options.nodeId !== "string" || options.nodeId.length === 0) {
        throw configurationError("nodeId is required and must be a non-empty string");
    }
    if (!isRecord(options.storage) || typeof options.storage.directory !== "string" || options.storage.directory.length === 0) {
        throw configurationError("storage.directory is required and must be a non-empty string");
    }
    if (!isRecord(options.cluster)) {
        throw configurationError("cluster is required and must select bootstrap or join");
    }
    if (options.cluster.mode === "bootstrap") {
        if (typeof options.cluster.clusterId !== "string" || options.cluster.clusterId.length === 0) {
            throw configurationError("cluster.clusterId is required for bootstrap mode");
        }
    } else if (options.cluster.mode === "join") {
        assertEndpoints("cluster.seeds", options.cluster.seeds);
    } else {
        throw configurationError("cluster.mode must be 'bootstrap' or 'join'");
    }
    if (!isRecord(options.peer)) {
        throw configurationError("peer is required and must carry endpoint and tls");
    }
    assertEndpoint("peer.endpoint", options.peer.endpoint);
    assertTls("peer.tls", options.peer.tls);
    if (options.expirySweepIntervalMs !== undefined) {
        assertPositiveInteger("expirySweepIntervalMs", options.expirySweepIntervalMs);
    }
}

function stateError(state: YydsState, action: string): YydsError {
    return new YydsError(`cannot ${action} while the node is '${state}'`, {
        code: "yyds.state.invalid",
        phase: action === "close" ? "close" : "open",
        execution: "not-sent",
        retry: "never",
    });
}

function parseWireAddress(value: string): TcpEndpoint {
    const separator = value.lastIndexOf(":");
    if (separator <= 0) throw nativeError(`malformed wire address '${value}'`, "yyds.listen.failed", "open", "unknown");
    const port = Number(value.slice(separator + 1));
    if (!Number.isInteger(port) || port <= 0) {
        throw nativeError(`malformed wire address '${value}'`, "yyds.listen.failed", "open", "unknown");
    }
    return { host: value.slice(0, separator) || LOOPBACK_HOST, port };
}

/** One application listener owned by a {@link Yyds} node. */
export class YydsListener {
    #service: ServiceBinding | null;
    readonly endpoint: TcpEndpoint;
    readonly address: string;
    #detach: () => void;

    constructor(service: ServiceBinding, address: string, endpoint: TcpEndpoint, detach: () => void) {
        this.#service = service;
        this.address = address;
        this.endpoint = endpoint;
        this.#detach = detach;
    }

    /** Whether this listener has been closed. */
    get closed(): boolean {
        return this.#service === null;
    }

    /** Stops accepting new sessions. Idempotent; never closes the owning node. */
    async close(): Promise<void> {
        const service = this.#service;
        if (service === null) return;
        this.#service = null;
        this.#detach();
        try {
            service.stop();
        } catch (error) {
            throw nativeError(error, "yyds.listener.close.failed", "close", "unknown");
        }
    }
}

interface NodeHandles {
    readonly lease: unknown;
    readonly executor: LocalExecutorBinding;
    readonly sweeper: ExpirySweeperBinding;
}

/** Durable YYDS node runtime. */
export class Yyds {
    readonly #options: YydsOptions;
    #state: YydsState = "new";
    #openPromise: Promise<void> | null = null;
    #closePromise: Promise<void> | null = null;
    #handles: NodeHandles | null = null;
    #listeners = new Set<YydsListener>();

    constructor(options: YydsOptions) {
        validateOptions(options);
        this.#options = options;
    }

    get nodeId(): string {
        return this.#options.nodeId;
    }

    /** The declared cluster id for bootstrap mode; `join` has no local cluster id until a native join primitive exists. */
    get clusterId(): string | null {
        return this.#options.cluster.mode === "bootstrap" ? this.#options.cluster.clusterId : null;
    }

    get state(): YydsState {
        return this.#state;
    }

    /**
     * Completes identity and storage initialization. Concurrent calls merge into
     * the same operation; a closed or failed node is terminal and needs a new object.
     */
    async open(): Promise<void> {
        if (this.#state === "open") return;
        if (this.#state === "opening" && this.#openPromise !== null) return this.#openPromise;
        if (this.#state !== "new") throw stateError(this.#state, "open");
        this.#state = "opening";
        this.#openPromise = this.#openInner();
        try {
            await this.#openPromise;
        } finally {
            this.#openPromise = null;
        }
    }

    async #openInner(): Promise<void> {
        if (this.#options.cluster.mode === "join") {
            this.#state = "failed";
            throw new YydsError(
                "joining an existing cluster requires the native join and peer transport primitives, which this build does not export",
                { code: "yyds.join.unavailable", phase: "open", execution: "not-sent", retry: "never" },
            );
        }
        const native = this.#loadNative();
        const directory = this.#options.storage.directory;
        try {
            const identity = new native.NodeIdentityHandle(this.clusterId as string, this.#options.nodeId);
            native.nodeInit(directory, identity);
            const lease = native.NodeLeaseHandle.acquire(directory, identity);
            const executor = new native.LocalExecutorHandle(directory, null);
            const sweeper = native.ExpirySweeperHandle.start(
                executor,
                this.#options.expirySweepIntervalMs ?? DEFAULT_EXPIRY_SWEEP_INTERVAL_MS,
            );
            this.#handles = { lease, executor, sweeper };
            this.#state = "open";
        } catch (error) {
            this.#handles = null;
            this.#state = "failed";
            throw nativeError(error, "yyds.open.failed", "open", "unknown");
        }
    }

    /**
     * Starts only the loopback application port (YY native wire) and returns an
     * independent {@link YydsListener}. Closing the listener never closes the node.
     */
    async listen(options: YydsListenOptions = {}): Promise<YydsListener> {
        if (this.#state !== "open" || this.#handles === null) throw stateError(this.#state, "listen");
        if (options.authentication !== undefined) {
            throw new YydsError(
                "application-port authentication is not wired into the native wire server; bootstrap tokens are only read from the YYDS_AUTH_TOKENS environment",
                { code: "yyds.listen.authentication.unavailable", phase: "open", execution: "not-sent", retry: "never" },
            );
        }
        const host = options.endpoint?.host ?? LOOPBACK_HOST;
        if (host !== LOOPBACK_HOST) {
            throw new YydsError("the native wire server binds the loopback interface only", {
                code: "yyds.listen.host.unsupported",
                phase: "open",
                execution: "not-sent",
                retry: "never",
            });
        }
        const port = options.endpoint?.port ?? 0;
        if (!Number.isInteger(port) || port < 0 || port > 65535) {
            throw configurationError("listen endpoint.port must be an integer between 0 and 65535");
        }
        const native = this.#loadNative();
        try {
            const service = native.WireServerHandle.startWithExecutor(port, this.#handles.executor);
            const address = service.address();
            let endpoint: TcpEndpoint;
            try {
                endpoint = parseWireAddress(address);
            } catch (error) {
                try {
                    service.stop();
                } catch {
                    // 地址解析失败时尽力回收已启动的服务,原始错误优先上报。
                }
                throw error;
            }
            const listener = new YydsListener(service, address, endpoint, () => {
                this.#listeners.delete(listener);
            });
            this.#listeners.add(listener);
            return listener;
        } catch (error) {
            if (error instanceof YydsError) throw error;
            throw nativeError(error, "yyds.listen.failed", "open", "unknown");
        }
    }

    /**
     * Reports what this node actually is. A single-node bootstrap never claims
     * cluster qualification: membership, replication, failover and reconfiguration
     * stay false until the native primitives exist.
     */
    capabilities(): YydsCapabilities {
        const mode = this.#options.cluster.mode;
        const open = this.#state === "open";
        return {
            product: "yyds",
            version: this.#nativeVersion(),
            identity: { nodeId: this.#options.nodeId, clusterId: this.clusterId, mode },
            lifecycle: { state: this.#state },
            storage: { directory: this.#options.storage.directory, persistent: true },
            cluster: {
                qualified: false,
                members: mode === "bootstrap" && open ? 1 : 0,
                replication: false,
                failover: false,
                reconfiguration: false,
                note:
                    mode === "bootstrap"
                        ? "single-node bootstrap; this object does not qualify as a production cluster"
                        : "join mode is reported as unavailable until the native join primitive exists",
            },
            peer: {
                transport: false,
                tls: false,
                note: "peer configuration is validated at construction; no native peer transport is exported yet",
            },
            application: { wireProtocol: open, authentication: "env-tokens-only" },
            gaps: YYDS_SURFACE_GAPS,
        };
    }

    /**
     * Cascades over owned resources: listeners first, then the expiry sweeper.
     * Idempotent. The lease and executor handles are released with garbage
     * collection because the native layer exports no explicit release primitive.
     */
    async close(): Promise<void> {
        if (this.#state === "closed") return;
        if (this.#closePromise !== null) return this.#closePromise;
        if (this.#state === "opening" && this.#openPromise !== null) {
            await this.#openPromise.catch(() => undefined);
        }
        if (this.#state === "new") {
            this.#state = "closed";
            return;
        }
        this.#state = "closing";
        this.#closePromise = this.#closeInner();
        return this.#closePromise;
    }

    async #closeInner(): Promise<void> {
        const failures: unknown[] = [];
        for (const listener of [...this.#listeners]) {
            try {
                await listener.close();
            } catch (error) {
                failures.push(error);
            }
        }
        const handles = this.#handles;
        this.#handles = null;
        if (handles !== null) {
            try {
                handles.sweeper.stop();
            } catch (error) {
                failures.push(error);
            }
        }
        this.#closePromise = null;
        this.#state = "closed";
        if (failures.length > 0) {
            throw nativeError(failures[0], "yyds.close.partial", "close", "unknown");
        }
    }

    #loadNative(): YydsBindings {
        try {
            return loadYydsNative();
        } catch (error) {
            throw nativeError(error, "yyds.native.unavailable", "open", "not-sent");
        }
    }

    #nativeVersion(): string | null {
        if (!isYydsNativeInstalled()) return null;
        try {
            return this.#loadNative().yydsVersion();
        } catch {
            return null;
        }
    }
}
