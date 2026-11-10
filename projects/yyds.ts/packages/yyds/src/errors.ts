/** Unified error envelope for the `@yydb/yyds` author surface. */

export type YydsErrorPhase = "configuration" | "open" | "connect" | "execute" | "commit" | "close";

/** Whether the failed operation ever reached the executor. */
export type YydsExecution = "not-sent" | "unknown" | "replied";

/** Retry guidance carried by the envelope, never an automatic retry instruction. */
export type YydsRetry = "never" | "safe" | "application-decision";

export interface YydsErrorDetails {
    readonly code: string;
    readonly phase: YydsErrorPhase;
    readonly execution?: YydsExecution;
    readonly retry?: YydsRetry;
    readonly cause?: unknown;
}

/** Every YYDS author-surface failure surfaces as this error shape. */
export class YydsError extends Error {
    readonly product = "yyds" as const;
    readonly code: string;
    readonly phase: YydsErrorPhase;
    readonly execution: YydsExecution;
    readonly retry: YydsRetry;

    constructor(message: string, details: YydsErrorDetails) {
        super(message, details.cause === undefined ? undefined : { cause: details.cause });
        this.name = "YydsError";
        this.code = details.code;
        this.phase = details.phase;
        this.execution = details.execution ?? "not-sent";
        this.retry = details.retry ?? "never";
    }
}

/** A rejected constructor or listen option shape. Nothing was sent anywhere. */
export function configurationError(detail: string): YydsError {
    return new YydsError(`invalid yyds configuration: ${detail}`, {
        code: "yyds.configuration.invalid",
        phase: "configuration",
        execution: "not-sent",
        retry: "never",
    });
}

/** Wraps a native or primitive failure into the envelope. */
export function nativeError(
    cause: unknown,
    code: string,
    phase: YydsErrorPhase,
    execution: YydsExecution,
    retry: YydsRetry = "never",
): YydsError {
    const message = cause instanceof Error ? cause.message : String(cause);
    return new YydsError(message, { code, phase, execution, retry, cause });
}
