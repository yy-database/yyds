/** Shared option shapes and light constructor-time validation for the author surface. */

import { configurationError } from "./errors.ts";

export interface TcpEndpoint {
    readonly host: string;
    readonly port: number;
}

export interface LocalStorageOptions {
    readonly directory: string;
}

export interface TlsIdentityOptions {
    readonly certificate: Uint8Array;
    readonly privateKey: Uint8Array;
}

export interface TlsOptions {
    readonly ca?: readonly Uint8Array[];
    readonly serverName?: string;
    readonly identity?: TlsIdentityOptions;
}

export interface PoolOptions {
    readonly min?: number;
    readonly max?: number;
    readonly maxPending?: number;
    readonly acquireTimeoutMs?: number;
    readonly idleTimeoutMs?: number;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function assertPort(label: string, value: unknown, allowEphemeral: boolean): void {
    if (typeof value !== "number" || !Number.isInteger(value)) {
        throw configurationError(`${label} must be an integer port`);
    }
    const minimum = allowEphemeral ? 0 : 1;
    if (value < minimum || value > 65535) {
        throw configurationError(`${label} must be between ${minimum} and 65535`);
    }
}

export function assertEndpoint(label: string, value: unknown, allowEphemeral = false): void {
    if (!isRecord(value)) throw configurationError(`${label} must be an object with host and port`);
    if (typeof value.host !== "string" || value.host.length === 0) {
        throw configurationError(`${label}.host must be a non-empty string`);
    }
    assertPort(`${label}.port`, value.port, allowEphemeral);
}

/** Empty endpoint lists are always invalid. */
export function assertEndpoints(label: string, value: unknown): readonly TcpEndpoint[] {
    if (!Array.isArray(value) || value.length === 0) {
        throw configurationError(`${label} must be a non-empty array of endpoints`);
    }
    value.forEach((endpoint, index) => assertEndpoint(`${label}[${index}]`, endpoint));
    return value as readonly TcpEndpoint[];
}

export function assertTls(label: string, value: unknown): void {
    if (!isRecord(value)) throw configurationError(`${label} must be an object`);
    if (value.ca !== undefined) {
        if (!Array.isArray(value.ca) || value.ca.some((entry) => !(entry instanceof Uint8Array))) {
            throw configurationError(`${label}.ca must be an array of byte buffers`);
        }
    }
    if (value.serverName !== undefined && (typeof value.serverName !== "string" || value.serverName.length === 0)) {
        throw configurationError(`${label}.serverName must be a non-empty string`);
    }
    if (value.identity !== undefined) {
        if (
            !isRecord(value.identity) ||
            !(value.identity.certificate instanceof Uint8Array) ||
            !(value.identity.privateKey instanceof Uint8Array)
        ) {
            throw configurationError(`${label}.identity must carry certificate and privateKey byte buffers`);
        }
    }
}

export function assertPool(label: string, value: unknown): void {
    if (!isRecord(value)) throw configurationError(`${label} must be an object`);
    for (const key of ["min", "max"] as const) {
        const entry = value[key];
        if (entry !== undefined && (typeof entry !== "number" || !Number.isInteger(entry) || entry < 1)) {
            throw configurationError(`${label}.${key} must be a positive integer`);
        }
    }
    for (const key of ["maxPending", "acquireTimeoutMs", "idleTimeoutMs"] as const) {
        const entry = value[key];
        if (entry !== undefined && (typeof entry !== "number" || !Number.isInteger(entry) || entry < 0)) {
            throw configurationError(`${label}.${key} must be a non-negative integer`);
        }
    }
    if (typeof value.min === "number" && typeof value.max === "number" && value.min > value.max) {
        throw configurationError(`${label}.min must not exceed ${label}.max`);
    }
}

export function assertPositiveInteger(label: string, value: unknown): void {
    if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
        throw configurationError(`${label} must be a positive integer`);
    }
}

export function assertNonNegativeInteger(label: string, value: unknown): void {
    if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
        throw configurationError(`${label} must be a non-negative integer`);
    }
}
