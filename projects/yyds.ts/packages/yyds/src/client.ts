/**
 * `YydsClient` — the remote application client owner.
 *
 * The constructor validates endpoints, auth, tls, pool and discovery configuration.
 * The client connects to application ports and would project the same VOS data
 * surface as `YydbClient` (table/prepare/transaction/capabilities), but this build
 * exports neither a native YY wire client primitive nor VOS data-plane frames, and
 * it never grants membership or reconfiguration rights. Those gaps are reported
 * honestly instead of being faked with a second protocol implementation in TypeScript.
 */

import { configurationError, YydsError } from "./errors.ts";
import {
    assertEndpoints,
    assertNonNegativeInteger,
    assertPool,
    assertPositiveInteger,
    assertTls,
    isRecord,
    type PoolOptions,
    type TcpEndpoint,
    type TlsOptions,
} from "./types.ts";

export type { PoolOptions, TcpEndpoint, TlsOptions };

export interface YydsAuth {
    readonly username: string;
    readonly password: string;
}

export interface YydsDiscoveryOptions {
    readonly refreshIntervalMs?: number;
    readonly maxRedirects?: number;
}

export interface YydsClientOptions {
    readonly endpoints: readonly TcpEndpoint[];
    readonly auth?: YydsAuth;
    readonly tls?: TlsOptions;
    readonly pool?: PoolOptions;
    readonly discovery?: YydsDiscoveryOptions;
    readonly connectTimeoutMs?: number;
}

export type YydsClientState = "new" | "connecting" | "ready" | "reconnecting" | "closing" | "closed" | "failed";

/**
 * Canonical VOS table descriptor. The real shape is the `Table` type from
 * `@voml/vos`; this package does not depend on `@voml/vos` yet because the
 * native wire protocol does not carry VOS operations. The parameter stays
 * `unknown` so callers pass the canonical descriptor without a second local type.
 */
export type YydsVosDescriptor = unknown;

export interface YydsClientCapabilities {
    readonly product: "yyds";
    readonly lifecycle: { readonly state: YydsClientState };
    readonly endpoints: readonly TcpEndpoint[];
    readonly configured: {
        readonly auth: boolean;
        readonly tls: boolean;
        readonly pool: boolean;
        readonly discovery: boolean;
    };
    readonly dataPlane: { readonly table: boolean; readonly prepare: boolean; readonly transaction: boolean };
    readonly management: { readonly membershipChanges: boolean; readonly reconfiguration: boolean };
    readonly gaps: readonly string[];
}

function validateOptions(options: unknown): void {
    if (!isRecord(options)) throw configurationError("YydsClient options must be an object");
    assertEndpoints("endpoints", options.endpoints);
    if (options.auth !== undefined) {
        if (!isRecord(options.auth) || typeof options.auth.username !== "string" || typeof options.auth.password !== "string") {
            throw configurationError("auth must carry string username and password");
        }
    }
    if (options.tls !== undefined) assertTls("tls", options.tls);
    if (options.pool !== undefined) assertPool("pool", options.pool);
    if (options.discovery !== undefined) {
        if (!isRecord(options.discovery)) throw configurationError("discovery must be an object");
        if (options.discovery.refreshIntervalMs !== undefined) {
            assertPositiveInteger("discovery.refreshIntervalMs", options.discovery.refreshIntervalMs);
        }
        if (options.discovery.maxRedirects !== undefined) {
            assertNonNegativeInteger("discovery.maxRedirects", options.discovery.maxRedirects);
        }
    }
    if (options.connectTimeoutMs !== undefined) {
        assertNonNegativeInteger("connectTimeoutMs", options.connectTimeoutMs);
    }
}

/** Remote client for YYDS application ports. */
export class YydsClient {
    readonly #endpoints: readonly TcpEndpoint[];
    readonly #configured: {
        readonly auth: boolean;
        readonly tls: boolean;
        readonly pool: boolean;
        readonly discovery: boolean;
    };
    #state: YydsClientState = "new";

    constructor(options: YydsClientOptions) {
        validateOptions(options);
        this.#endpoints = (options.endpoints as readonly TcpEndpoint[]).map((endpoint) => ({ ...endpoint }));
        this.#configured = {
            auth: options.auth !== undefined,
            tls: options.tls !== undefined,
            pool: options.pool !== undefined,
            discovery: options.discovery !== undefined,
        };
    }

    get state(): YydsClientState {
        return this.#state;
    }

    /**
     * Would open the connection pool against the configured application endpoints.
     * This build does not export a native YY wire client primitive, so connecting
     * fails with an honest gap error; nothing is sent and no fallback transport is
     * invented in TypeScript.
     */
    async connect(): Promise<void> {
        if (this.#state !== "new") {
            throw new YydsError(`cannot connect while the client is '${this.#state}'`, {
                code: "yyds.client.state.invalid",
                phase: "connect",
                execution: "not-sent",
                retry: "never",
            });
        }
        this.#state = "connecting";
        this.#state = "failed";
        throw new YydsError(
            "connecting requires the native YY wire client primitive, which this build does not export",
            { code: "yyds.client.transport.unavailable", phase: "connect", execution: "not-sent", retry: "never" },
        );
    }

    /** VOS table projection. Requires a ready connection; see {@link capabilities} for the gap list. */
    async table(_descriptor: YydsVosDescriptor): Promise<never> {
        throw this.#dataPlaneUnavailable("table");
    }

    /** VOS prepared query projection. Requires a ready connection; see {@link capabilities}. */
    async prepare(_request: YydsVosDescriptor): Promise<never> {
        throw this.#dataPlaneUnavailable("prepare");
    }

    /** VOS transaction projection. The callback would run exactly once against a ready connection. */
    async transaction<T>(_fn: (tx: unknown) => Promise<T>): Promise<T> {
        throw this.#dataPlaneUnavailable("transaction");
    }

    /**
     * Reports what this client can actually do. Data-plane projections are false
     * until the wire protocol carries VOS operations, and a client never holds
     * membership or reconfiguration rights.
     */
    capabilities(): YydsClientCapabilities {
        return {
            product: "yyds",
            lifecycle: { state: this.#state },
            endpoints: this.#endpoints.map((endpoint) => ({ ...endpoint })),
            configured: { ...this.#configured },
            dataPlane: { table: false, prepare: false, transaction: false },
            management: { membershipChanges: false, reconfiguration: false },
            gaps: [
                "client.transport: no native YY wire client primitive for connect()",
                "client.dataPlane: VOS table/prepare/transaction are not carried by the wire protocol yet",
                "client.discovery: no native topology discovery or redirect primitive to refresh endpoints",
            ],
        };
    }

    /**
     * Closes the connection pool and every derived resource. The remote node is
     * never affected. Idempotent.
     */
    async close(): Promise<void> {
        this.#state = "closed";
    }

    #dataPlaneUnavailable(operation: string): YydsError {
        return new YydsError(
            `cannot run '${operation}' while the client is '${this.#state}'; connect() cannot reach 'ready' until the native wire client and VOS data-plane primitives exist`,
            { code: "yyds.client.not_connected", phase: "execute", execution: "not-sent", retry: "never" },
        );
    }
}
