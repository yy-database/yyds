/** Shared Node and WASM binding surface for `@yydb/yyds`. */
export interface YydsBindings {
    yydsVersion(): string;
    ping(): string;
    NodeIdentityHandle: new (clusterId: string, nodeId: string) => unknown;
    NodeLeaseHandle: {
        acquire(dataDir: string, identity: unknown): unknown;
    };
    LocalExecutorHandle: new (dataDir: string, catalogPath?: string | null) => LocalExecutorBinding;
    SqlSchemaRegistryHandle: {
        open(path: string): unknown;
    };
    ExpirySweeperHandle: {
        start(executor: LocalExecutorBinding, intervalMs: number): ExpirySweeperBinding;
    };
    RedisServiceHandle: new (
        lease: unknown,
        port: number,
        executor: unknown,
    ) => RedisServiceBinding;
    PgsqlServiceHandle: {
        new (lease: unknown, port: number): PgsqlServiceBinding;
        startWithExecutor(
            lease: unknown,
            port: number,
            executor: unknown,
            catalogPath: string,
        ): PgsqlServiceBinding;
        startWithSharedSchema(
            lease: unknown,
            port: number,
            executor: unknown,
            schema: unknown,
        ): PgsqlServiceBinding;
    };
    MysqlServiceHandle: {
        new (lease: unknown, port: number): MysqlServiceBinding;
        startWithExecutor(
            lease: unknown,
            port: number,
            executor: unknown,
            catalogPath: string,
        ): MysqlServiceBinding;
        startWithSharedSchema(
            lease: unknown,
            port: number,
            executor: unknown,
            schema: unknown,
        ): MysqlServiceBinding;
    };
    MongoServiceHandle: new (
        lease: unknown,
        port: number,
        executor: unknown,
    ) => MongoServiceBinding;
    WireServerHandle: new (port: number, executor: unknown) => WireServiceBinding;
    catalogPublishSchema(catalogPath: string, schemaVersion: number, schemaDocument: string): void;
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

export interface LocalExecutorBinding {
    /** Reclaims every expired record once and returns how many were removed. */
    sweepExpired(): number;
    /**
     * Runs one budgeted reclamation pass, inspecting at most `budgetPerShard`
     * records per shard. A pass that hits the budget resumes from its key
     * cursor on the next call instead of re-scanning the shard head.
     */
    sweepExpiredBudgeted(budgetPerShard: number): SweepOutcomeBinding;
}

/** One budgeted reclamation pass. */
export interface SweepOutcomeBinding {
    /** Records inspected in this pass. */
    examined: number;
    /** Records physically removed in this pass. */
    reclaimed: number;
}

export interface ExpirySweeperBinding {
    stop(): void;
}

export type RedisServiceBinding = ServiceBinding;
export type PgsqlServiceBinding = ServiceBinding;
export type MysqlServiceBinding = ServiceBinding;
export type MongoServiceBinding = ServiceBinding;
export type WireServiceBinding = ServiceBinding;

export interface NodeStatusBinding {
    directory: string;
    ownership: string;
    health: string;
    identity: string;
    runtime: string | null;
}
