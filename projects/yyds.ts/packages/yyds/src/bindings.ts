/** Shared Node and WASM binding surface for `@yydb/yyds`. */
export interface YydsBindings {
    yydsVersion(): string;
    ping(): string;
    NodeIdentityHandle: new (clusterId: string, nodeId: string) => unknown;
    NodeLeaseHandle: {
        acquire(dataDir: string, identity: unknown): unknown;
    };
    LocalExecutorHandle: new (dataDir: string) => unknown;
    RedisServiceHandle: new (
        lease: unknown,
        port: number,
        executor: unknown,
    ) => RedisServiceBinding;
    PgsqlServiceHandle: new (lease: unknown, port: number) => PgsqlServiceBinding;
    MysqlServiceHandle: new (lease: unknown, port: number) => MysqlServiceBinding;
    WireServerHandle: new (port: number, executor: unknown) => WireServiceBinding;
    nodeInit(dataDir: string, identity: unknown): void;
    nodeInspect(dataDir: string): NodeStatusBinding;
    nodeReadRuntimeStatus(dataDir: string): string | null;
    nodeWriteRuntimeStatus(dataDir: string, statusJson: string): void;
    nodeRequestStop(dataDir: string, generation: string): void;
    nodeWaitUntilStopped(dataDir: string, generation: string, timeoutMs: number): void;
    nodeStopRequested(dataDir: string, generation: string): boolean;
    nodeClearRuntimeArtifacts(dataDir: string, generation: string): void;
}

export interface ServiceBinding {
    address(): string;
    isFinished(): boolean;
    stop(): void;
}

export type RedisServiceBinding = ServiceBinding;
export type PgsqlServiceBinding = ServiceBinding;
export type MysqlServiceBinding = ServiceBinding;
export type WireServiceBinding = ServiceBinding;

export interface NodeStatusBinding {
    directory: string;
    ownership: string;
    health: string;
    identity: string;
    runtime: string | null;
}
