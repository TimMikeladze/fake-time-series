import fs from "node:fs";
import path from "node:path";
import { setTimeout as sleepMs } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import type { Command } from "commander";
import ms from "ms";
import type {
	FakeTimeSeriesOptions,
	FakeTimeSeriesToSinkOptions,
	TransformFunction,
} from "./index";

const DEFAULT_CONFIG_FILES = [
	"fake-time-series.config.mjs",
	"fake-time-series.config.js",
];

// Tell vite's SSR transform to leave this dynamic import alone (it otherwise
// tries to resolve arbitrary file:// URLs at transform time and fails for
// paths that aren't known at build). At runtime, Node's native loader handles
// the file:// URL correctly.
const dynamicImport = (specifier: string): Promise<Record<string, unknown>> =>
	import(/* @vite-ignore */ specifier);

export type CliConfig = Partial<
	FakeTimeSeriesOptions &
		Omit<FakeTimeSeriesToSinkOptions, "fetcher"> & {
			headers?: Record<string, string>;
			sinkUrl?: string;
			transform?: TransformFunction;
		}
>;

/**
 * Pure path-resolution step for {@link loadConfig}. Decides which file on
 * disk should be loaded (if any) without actually importing it. Split out
 * so the existence/extension-fallback logic can be unit-tested without
 * going through the dynamic import machinery that vitest intercepts
 * in-process.
 *
 * - If `configPath` is given, try that path first. If it has no `.mjs`/`.js`
 *   extension, try appending each one. Throws if none of the candidates
 *   exist.
 * - If `configPath` is omitted, walk the default config filenames in `cwd`.
 *   Returns `null` if none exist — the CLI must work without a config file.
 */
export function resolveConfigPath(
	configPath: string | undefined,
	cwd: string = process.cwd(),
	existsSync: (p: string) => boolean = fs.existsSync,
): string | null {
	if (configPath) {
		const resolved = path.resolve(cwd, configPath);
		const hasExtension = resolved.endsWith(".mjs") || resolved.endsWith(".js");
		const candidates = hasExtension
			? [resolved]
			: [`${resolved}.mjs`, `${resolved}.js`];

		for (const candidate of candidates) {
			if (existsSync(candidate)) {
				return candidate;
			}
		}
		throw new Error(
			`Config file not found: ${configPath} (tried ${candidates.join(", ")})`,
		);
	}

	for (const defaultName of DEFAULT_CONFIG_FILES) {
		const candidate = path.resolve(cwd, defaultName);
		if (existsSync(candidate)) {
			return candidate;
		}
	}

	return null;
}

/**
 * Load a config file.
 *
 * - If `configPath` is given, require it to exist. Try the given path, then
 *   `.mjs` and `.js` extensions if no extension was specified.
 * - If `configPath` is omitted, look for the default config files in cwd.
 *   Return an empty object if none exist — the CLI must work without a
 *   config file.
 */
export async function loadConfig(configPath?: string): Promise<CliConfig> {
	const resolvedPath = resolveConfigPath(configPath);
	if (!resolvedPath) {
		return {};
	}
	const mod = await dynamicImport(pathToFileURL(resolvedPath).href);
	return (mod.options as CliConfig | undefined) ?? {};
}

// Accept `N` or `N.0+` only — no trailing characters, no scientific
// notation, no negative sign. This is stricter than `parseFloat`, which
// silently accepts "42abc" as 42 and "4e2" as 400.
const POSITIVE_INT_PATTERN = /^\d+(?:\.0+)?$/;

// Accept plain decimal numbers only (e.g. "0", "1", "0.5", "0.999"). No
// trailing characters, no scientific notation, no sign. Stricter than
// `parseFloat`, which accepts "0.5 xyz" as 0.5 via partial parse.
const DECIMAL_PATTERN = /^\d+(?:\.\d+)?$/;

/**
 * Commander parser for a positive integer flag. Accepts `"4"` and `"4.0"`
 * (both parse as `4`) and rejects anything else — `"4.5"`, `"abc"`, `"0"`,
 * `"-1"`, `"42abc"`, `"4e2"`, `"Infinity"`, etc.
 *
 * Throws a descriptive error for invalid input so it surfaces as a
 * commander validation error rather than a silent NaN or a silent
 * partial parse.
 */
export const parsePositiveInt =
	(name: string) =>
	(value: string): number => {
		const trimmed = value.trim();
		if (!POSITIVE_INT_PATTERN.test(trimmed)) {
			throw new Error(`${name} must be a positive integer (got "${value}")`);
		}
		const parsed = Number.parseFloat(trimmed);
		if (!Number.isFinite(parsed) || parsed <= 0) {
			throw new Error(`${name} must be a positive integer (got "${value}")`);
		}
		return parsed;
	};

/**
 * Commander parser for a probability flag in [0, 1]. Uses the same
 * regex-gated approach as {@link parsePositiveInt} so partial parses
 * (e.g. `"0.5 xyz"` → 0.5) and scientific notation are rejected rather
 * than silently accepted.
 */
export const parseProbability =
	(name: string) =>
	(value: string): number => {
		const trimmed = value.trim();
		if (!DECIMAL_PATTERN.test(trimmed)) {
			throw new Error(
				`${name} must be a number between 0 and 1 (got "${value}")`,
			);
		}
		const parsed = Number.parseFloat(trimmed);
		if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
			throw new Error(
				`${name} must be a number between 0 and 1 (got "${value}")`,
			);
		}
		return parsed;
	};

// Runtime-safe cast of the `ms()` API. Its public typings restrict the
// argument to a narrow `StringValue` template literal, but at runtime it
// happily accepts any string and returns `undefined` for invalid input.
// We validate the undefined return below, so this boundary cast is the
// honest, type-safe-at-the-edge choice. Mirrors the pattern used by
// `parseInterval` in src/index.ts.
const msParseDuration = ms as unknown as (value: string) => number | undefined;

// Accept raw non-negative millisecond integers. Stricter than
// POSITIVE_INT_PATTERN because we want to allow "0" here (a 0ms tick
// interval is a legitimate "re-tick immediately" signal in live mode),
// which parsePositiveInt rejects.
const NON_NEGATIVE_INT_PATTERN = /^\d+$/;

/**
 * Commander parser for a duration flag expressed in milliseconds.
 * Accepts either a non-negative integer (interpreted as milliseconds)
 * or a human-readable duration that `ms()` understands
 * (`"1s"`, `"500ms"`, `"2m"`, …). Returns the duration in milliseconds.
 *
 * Rejects: negative values, non-numeric garbage, partial parses
 * (`"1s foo"`), and non-finite results.
 *
 * Used by the `--live-interval` flag in live mode; split out so the
 * parser can be unit-tested in isolation from commander wiring.
 */
export const parseDurationMs =
	(name: string) =>
	(value: string): number => {
		const trimmed = value.trim();

		const invalid = (): Error =>
			new Error(
				`${name} must be a non-negative duration ` +
					`(e.g. "1s", "500ms", or a millisecond integer; got "${value}")`,
			);

		// Empty / whitespace-only input: guard BEFORE touching `ms()`.
		// `ms("")` throws a raw library error ("val is not a non-empty
		// string or a valid number") which would otherwise leak through
		// and override our friendly error message. Whitespace-only
		// inputs collapse to "" after `trim()` and hit the same path.
		if (trimmed.length === 0) {
			throw invalid();
		}

		// Fast path: plain non-negative integer → milliseconds.
		if (NON_NEGATIVE_INT_PATTERN.test(trimmed)) {
			const parsed = Number.parseInt(trimmed, 10);
			if (Number.isFinite(parsed) && parsed >= 0) {
				return parsed;
			}
		}

		// Reject any interior whitespace BEFORE delegating to `ms()`.
		// Two reasons:
		//   1. `ms("1 s")` returns 1000 — the library silently accepts a
		//      space between the number and the unit. Users of this CLI
		//      should always write the canonical form ("1s" / "500ms"),
		//      so we draw a firm boundary here.
		//   2. Rejecting interior whitespace defangs any future `ms`
		//      release that might start accepting "1 s foo" as 1000 ms
		//      with a partial parse. Gating on whitespace first makes
		//      the intent obvious instead of hiding a post-parse check
		//      that could look like dead code to a later reader.
		if (/\s/.test(trimmed)) {
			throw invalid();
		}

		// Fall back to `ms()` for human-readable durations. `ms()` returns
		// `undefined` for garbage and accepts negative values (e.g. "-1s"
		// → -1000), which we must reject.
		const parsed = msParseDuration(trimmed);
		if (parsed === undefined || !Number.isFinite(parsed) || parsed < 0) {
			throw invalid();
		}

		return parsed;
	};

/**
 * Read the `--live-interval` value off a commander `Command` and
 * validate it at the CLI boundary. `getOptionValue` is typed `unknown`
 * because commander can store arbitrary values there, so this helper
 * narrows it to `number` with a runtime check — a non-number at this
 * point indicates a wiring bug (e.g. `parseDurationMs` was unhooked
 * from the option, or a future commander upgrade changed its coercion
 * path) and we surface a clear error instead of letting an `as number`
 * cast paper over it.
 *
 * Exported so the wiring-bug guard is independently testable without
 * setting up a full commander pipeline.
 */
export const readLiveIntervalMs = (command: Command): number => {
	const raw = command.getOptionValue("liveInterval");
	if (typeof raw !== "number" || !Number.isFinite(raw) || raw < 0) {
		throw new Error(
			`Internal error: --live-interval must resolve to a non-negative finite number (got ${String(
				raw,
			)})`,
		);
	}
	return raw;
};

/**
 * Options accepted by {@link runLive}.
 *
 * Every hazardous external dependency (clock, sleep, signal) is injectable
 * so the loop is deterministically testable without waiting on wall-clock
 * time or spawning child processes. Production wiring plugs in `Date`,
 * `timers/promises#setTimeout`, and a SIGINT-driven AbortController.
 */
export interface RunLiveOptions {
	/**
	 * Resolved start time for the first window. Must be an absolute Date —
	 * callers are responsible for resolving relative strings (e.g. "-1 day")
	 * exactly once BEFORE the loop begins, so the window doesn't drift.
	 */
	initialStartTime: Date;
	/** Milliseconds to sleep between ticks. Must be finite and >= 0. */
	intervalMs: number;
	/** Signal that terminates the loop cleanly when aborted. */
	signal: AbortSignal;
	/**
	 * Called once per tick with the current window `[start, end]`. Errors
	 * thrown here propagate up and terminate the loop; transient errors
	 * (e.g. a single failed HTTP request) should be handled inside `tick`.
	 */
	tick: (windowStart: Date, windowEnd: Date) => Promise<void>;
	/** Clock injection for tests. Defaults to `() => new Date()`. */
	now?: () => Date;
	/**
	 * Sleep injection for tests. Defaults to `timers/promises#setTimeout`
	 * with the abort signal wired in, so a real SIGINT wakes the loop
	 * immediately instead of waiting out the rest of the interval.
	 */
	sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
}

/**
 * Default sleep implementation for live mode. Wraps
 * `timers/promises#setTimeout` and swallows the `AbortError` that fires
 * when the signal is aborted mid-sleep — a user-initiated SIGINT is
 * expected behavior, not a failure mode.
 */
const defaultLiveSleep = async (
	intervalMs: number,
	signal: AbortSignal,
): Promise<void> => {
	try {
		await sleepMs(intervalMs, undefined, { signal });
	} catch (error: unknown) {
		// AbortError is the expected path during graceful shutdown. Any
		// other error is a real bug and must surface.
		//
		// Node's `timers/promises#setTimeout` throws a `DOMException`
		// on abort (not an `Error` subclass in every environment), so
		// we match on `.name` rather than `instanceof Error`. The
		// `"AbortError"` name is stable across both the `Error` and
		// `DOMException` paths per the WHATWG abort spec.
		if (
			typeof error === "object" &&
			error !== null &&
			(error as { name?: unknown }).name === "AbortError"
		) {
			return;
		}
		throw error;
	}
};

/**
 * Run a tick function repeatedly in a non-overlapping, non-lossy window
 * loop until the provided `AbortSignal` fires.
 *
 * Window semantics:
 *   * First window is `[initialStartTime, now()]`.
 *   * Each subsequent window is `[previousEnd, now()]` — so no data is
 *     missed (no gap) and no data is double-counted (no overlap).
 *   * If the window is empty (`now() <= previousEnd`), the tick is
 *     skipped for this iteration, but the loop still sleeps and
 *     re-checks on the next iteration.
 *
 * Shutdown semantics:
 *   * `signal.aborted === true` on entry → returns without ticking.
 *   * Signal fires DURING a tick → the loop checks immediately after
 *     the tick returns and exits without sleeping.
 *   * Signal fires DURING the sleep → default sleep unblocks via the
 *     signal wired into `timers/promises#setTimeout`.
 *
 * Errors thrown by `tick` propagate up and terminate the loop. Callers
 * that want "log and continue" behavior should wrap their tick body in
 * a try/catch.
 */
export async function runLive(opts: RunLiveOptions): Promise<void> {
	if (!Number.isFinite(opts.intervalMs) || opts.intervalMs < 0) {
		throw new Error(
			`intervalMs must be a non-negative finite number (got ${opts.intervalMs})`,
		);
	}

	const now = opts.now ?? (() => new Date());
	const sleep = opts.sleep ?? defaultLiveSleep;

	let windowStart = opts.initialStartTime;

	while (!opts.signal.aborted) {
		const windowEnd = now();
		if (windowEnd.getTime() > windowStart.getTime()) {
			await opts.tick(windowStart, windowEnd);
			windowStart = windowEnd;
		}
		// Re-check BEFORE sleeping so an abort that arrived during the
		// tick doesn't make us pay an unnecessary full interval before
		// exiting.
		if (opts.signal.aborted) {
			break;
		}
		await sleep(opts.intervalMs, opts.signal);
	}
}

/**
 * Walk up from the script location to find the nearest `package.json`
 * that declares `fake-time-series` as a `bin` entry and return its
 * version. This works whether the package has been renamed by a fork
 * (as long as the `bin` entry is preserved) and whether the CLI is
 * invoked via a symlink, a direct path, or a bundled build.
 *
 * Returns `"unknown"` if no matching package.json is found — an
 * explicit sentinel is friendlier than a made-up `0.0.0`.
 */
export function readVersion(): string {
	try {
		const scriptPath = process.argv[1];
		if (!scriptPath) {
			return "unknown";
		}
		let dir: string;
		try {
			dir = path.dirname(fs.realpathSync(scriptPath));
		} catch {
			dir = path.dirname(scriptPath);
		}
		// Walk from the script's directory up to the filesystem root.
		// Using `parent === dir` as the terminator means there's no magic
		// depth limit — deeply nested monorepos still work.
		while (true) {
			const pkgPath = path.join(dir, "package.json");
			if (fs.existsSync(pkgPath)) {
				try {
					const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf-8")) as {
						name?: string;
						version?: string;
						bin?: string | Record<string, string>;
					};
					const declaresBin =
						pkg.bin != null &&
						(typeof pkg.bin === "string" ||
							Object.hasOwn(pkg.bin, "fake-time-series"));
					if (declaresBin && typeof pkg.version === "string") {
						return pkg.version;
					}
				} catch {
					// keep walking
				}
			}
			const parent = path.dirname(dir);
			if (parent === dir) {
				break;
			}
			dir = parent;
		}
	} catch {
		// fall through
	}
	return "unknown";
}

/**
 * Collect only the options explicitly set by the user on the command line
 * (or via env/config), excluding values left at their commander defaults.
 * This lets the config file supply a value when the CLI flag wasn't passed.
 */
export const collectExplicitOptions = (
	cmd: Command,
): Record<string, unknown> => {
	const result: Record<string, unknown> = {};
	for (const opt of cmd.options) {
		const attr = opt.attributeName();
		const source = cmd.getOptionValueSource(attr);
		if (source && source !== "default" && source !== "implied") {
			result[attr] = cmd.getOptionValue(attr);
		}
	}
	return result;
};
