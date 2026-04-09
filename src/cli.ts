#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { Command } from "commander";
import {
	collectExplicitOptions,
	loadConfig,
	parseDurationMs,
	parsePositiveInt,
	parseProbability,
	readLiveIntervalMs,
	readVersion,
	runLive,
} from "./cli-helpers";
import {
	type FakeTimeSeriesData,
	type FakeTimeSeriesOptions,
	type FakeTimeSeriesResult,
	type FakeTimeSeriesToSinkOptions,
	generate,
	parseTime,
	toSink,
} from "./index";

/**
 * Default tick interval for `--live` mode when the user doesn't pass
 * `--live-interval`. One second is a friendly middle ground: fast enough
 * that `fake-time-series generate --live | jq` feels responsive, slow
 * enough that a default invocation doesn't spin the CPU.
 */
const DEFAULT_LIVE_INTERVAL_MS = 1000;

/**
 * Default library-level startTime when the user doesn't pass a flag and
 * there's no config value. Mirrors `defaultOptions.startTime` in
 * `src/index.ts`; we need it explicitly here because `runLive` has to
 * resolve the initial window start ONCE before entering the loop (see
 * the extended comment in the action handlers below).
 */
const DEFAULT_START_TIME = "-1 day";

/**
 * Reject `--endTime X --live` at action-handler entry. Without this
 * guard, the user's `endTime` would be silently overwritten by
 * `windowEnd = now()` on every tick — a footgun where the user thinks
 * they asked for bounded streaming but gets an unbounded loop instead.
 *
 * Only the CLI-explicit case triggers: a config file that happens to
 * set `endTime` does NOT trip this guard, because configs are often
 * shared across use cases and silently overriding a config value in
 * live mode is the lesser evil compared to refusing to run at all.
 */
const assertLiveEndTimeCompat = (cliOptions: Record<string, unknown>): void => {
	if (cliOptions.endTime !== undefined) {
		throw new Error(
			"--endTime is incompatible with --live mode " +
				"(live mode streams indefinitely until SIGINT/SIGTERM). " +
				"Remove one of the flags.",
		);
	}
};

/**
 * Options accepted by {@link buildProgram} — lets tests inject stubs for
 * the library runners and override the error handler so commander doesn't
 * call `process.exit` under test.
 */
export interface BuildProgramOptions {
	generateRunner?: (
		options: FakeTimeSeriesOptions,
	) => Promise<FakeTimeSeriesResult>;
	toSinkRunner?: (
		options: FakeTimeSeriesToSinkOptions,
	) => Promise<FakeTimeSeriesResult>;
	/**
	 * Called when an action handler throws. The default logs to stderr and
	 * calls `process.exit(1)`. Tests should inject a handler that re-throws
	 * so `parseAsync` rejects instead of tearing down the process.
	 */
	onError?: (error: unknown) => void;
	/**
	 * Called by `toSink` for each per-batch error (non-2xx response,
	 * fetcher exception). Only used by the `send` command, and only when
	 * the loaded config file does NOT itself provide an `onError`.
	 * Defaults to a handler that logs to stderr.
	 */
	sinkBatchErrorHandler?: (error: unknown) => void;
	/** Optional stdout sink for action output. Defaults to `console.log`. */
	log?: (message: string) => void;
	/**
	 * Signal that terminates `--live` mode's tick loop. The production
	 * entry point wires this to `SIGINT` / `SIGTERM`; tests inject their
	 * own controller so they can deterministically stop the loop after
	 * a fixed number of ticks. If omitted, a fresh never-aborted signal
	 * is used — which means `--live` runs forever, so tests that exercise
	 * live mode MUST provide their own signal.
	 */
	liveSignal?: AbortSignal;
	/**
	 * Clock injection for `--live` mode. Passed through to {@link runLive}.
	 * Defaults to `() => new Date()`. Tests use this to drive a fake clock.
	 */
	now?: () => Date;
	/**
	 * Sleep injection for `--live` mode. Passed through to {@link runLive}.
	 * Defaults to `timers/promises#setTimeout` wired to the abort signal.
	 * Tests use this to skip real wall-clock waits.
	 */
	sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
}

const defaultErrorHandler = (error: unknown): void => {
	const message = error instanceof Error ? error.message : String(error);
	// biome-ignore lint/suspicious/noConsole: CLI error output
	console.error(`Error: ${message}`);
	process.exit(1);
};

const defaultSinkBatchErrorHandler = (error: unknown): void => {
	const message = error instanceof Error ? error.message : String(error);
	// biome-ignore lint/suspicious/noConsole: CLI error output
	console.error(`Error: ${message}`);
};

// biome-ignore lint/suspicious/noConsole: CLI stdout default
const defaultLog = (message: string): void => console.log(message);

const withCommonOptions = (command: Command): Command =>
	command
		.option("-s, --startTime <date>", "Start time of the series", "-1 day")
		.option("-e, --endTime <date>", "End time of the series")
		.option(
			"--minInterval <interval>",
			"Minimum interval between data points",
			"1s",
		)
		.option(
			"--maxInterval <interval>",
			"Maximum interval between data points",
			"10s",
		)
		.option(
			"--maxBatchSize <number>",
			"Maximum number of data points in a batch",
			parsePositiveInt("--maxBatchSize"),
			10,
		)
		.option("--no-batchSizeRandomization", "Disable batch size randomization")
		.option("--no-intervalRandomization", "Disable interval randomization")
		.option(
			"--batchReverseProbability <number>",
			"Probability to reverse each batch",
			parseProbability("--batchReverseProbability"),
			0.5,
		)
		.option(
			"--batchShuffleProbability <number>",
			"Probability to shuffle each batch",
			parseProbability("--batchShuffleProbability"),
			0.4,
		)
		.option(
			"--intervalSkewProbability <number>",
			"Probability of interval skewing",
			parseProbability("--intervalSkewProbability"),
			0.8,
		)
		.option("-c, --config <path>", "Path to config file")
		.option(
			"--live",
			"Continuously stream data in a loop (one tick per --live-interval) until the process receives SIGINT/SIGTERM",
		)
		.option(
			"--live-interval <duration>",
			'Interval between ticks in --live mode (e.g. "1s", "500ms", or raw ms integer)',
			parseDurationMs("--live-interval"),
			DEFAULT_LIVE_INTERVAL_MS,
		);

/**
 * Construct a fresh commander `Command` tree for the `fake-time-series`
 * CLI. Separated from the parse/run step so tests can build the program,
 * call `parseAsync(["node", "cli", ...])` with controlled argv, and
 * inspect results without the module's entry-point side effect firing.
 */
export function buildProgram(opts: BuildProgramOptions = {}): Command {
	const generateRunner = opts.generateRunner ?? generate;
	const toSinkRunner = opts.toSinkRunner ?? toSink;
	const onError = opts.onError ?? defaultErrorHandler;
	const sinkBatchErrorHandler =
		opts.sinkBatchErrorHandler ?? defaultSinkBatchErrorHandler;
	const log = opts.log ?? defaultLog;
	// If no live signal is provided we create a fresh never-aborted one;
	// it's only consulted on the `--live` code path, so the default case
	// (no --live flag) costs exactly one AbortController allocation.
	const liveSignal = opts.liveSignal ?? new AbortController().signal;

	const program = new Command();
	program
		.name("fake-time-series")
		.description("CLI for generating and sending fake time-series data.")
		.version(readVersion());

	const generateCommand = withCommonOptions(program.command("generate"));
	generateCommand
		.description("Generate fake time-series data")
		.action(async (_actionOptions, command: Command) => {
			try {
				const configPath = command.getOptionValue("config") as
					| string
					| undefined;
				const configOptions = await loadConfig(configPath);
				const cliOptions = collectExplicitOptions(command);
				delete cliOptions.config;
				// `--live` and `--live-interval` are CLI-only controls for
				// the tick loop — they must NOT leak into the library options
				// spread below, otherwise unknown properties would surface
				// in `FakeTimeSeriesOptions` and potentially confuse future
				// config consumers.
				const liveMode = cliOptions.live === true;
				if (liveMode) {
					// Must run BEFORE we delete `endTime` (we don't — but
					// also before live/liveInterval are deleted, purely for
					// readability: all live-mode validation lives here).
					assertLiveEndTimeCompat(cliOptions);
				}
				delete cliOptions.live;
				delete cliOptions.liveInterval;

				// CLI-explicit options override config; config overrides library defaults.
				const parsedOptions: FakeTimeSeriesOptions = {
					...configOptions,
					...cliOptions,
				};

				if (liveMode) {
					const intervalMs = readLiveIntervalMs(command);
					// CRITICAL: resolve `startTime` exactly ONCE up front.
					// A relative string like "-1 day" must NOT be re-parsed
					// every tick — that would anchor every window to
					// "1 day ago (relative to current now)" and trap live
					// mode in the same window forever.
					//
					// `parsedOptions` already spreads `configOptions`, so we
					// only need the CLI/config-merged value plus a library
					// default — no separate `configOptions.startTime` arm.
					// The `??` chain narrows `undefined` away so parseTime
					// gets a concrete input without any type-widening cast.
					const initialStartTime = parseTime(
						parsedOptions.startTime ?? DEFAULT_START_TIME,
					);
					await runLive({
						initialStartTime,
						intervalMs,
						signal: liveSignal,
						now: opts.now,
						sleep: opts.sleep,
						tick: async (windowStart, windowEnd) => {
							const tickOptions: FakeTimeSeriesOptions = {
								...parsedOptions,
								startTime: windowStart,
								endTime: windowEnd,
							};
							const result = await generateRunner(tickOptions);
							log(JSON.stringify(result, null, 2));
						},
					});
					return;
				}

				const result = await generateRunner(parsedOptions);
				log(JSON.stringify(result, null, 2));
			} catch (error) {
				onError(error);
			}
		});

	const sendCommand = withCommonOptions(program.command("send"));
	sendCommand
		.description("Generate fake time-series data and send it to a sink")
		.option(
			"--concurrency <number>",
			"Maximum number of concurrent requests",
			parsePositiveInt("--concurrency"),
			10,
		)
		.option("--sink-url <url>", "URL of the sink to send data")
		.option(
			"--headers <json>",
			"JSON string of headers to include in each request",
		)
		.action(async (_actionOptions, command: Command) => {
			try {
				const configPath = command.getOptionValue("config") as
					| string
					| undefined;
				const configOptions = await loadConfig(configPath);
				const cliOptions = collectExplicitOptions(command);
				delete cliOptions.config;
				// These are CLI-only and shouldn't be spread into library options.
				const cliSinkUrl = cliOptions.sinkUrl as string | undefined;
				const cliHeadersRaw = cliOptions.headers as string | undefined;
				delete cliOptions.sinkUrl;
				delete cliOptions.headers;
				// `--live` / `--live-interval` are CLI-only (same reasoning
				// as the generate command above).
				const liveMode = cliOptions.live === true;
				if (liveMode) {
					assertLiveEndTimeCompat(cliOptions);
				}
				delete cliOptions.live;
				delete cliOptions.liveInterval;

				const sinkUrl = cliSinkUrl ?? configOptions.sinkUrl;
				if (!sinkUrl) {
					throw new Error(
						"Sink URL is required (use --sink-url or set sinkUrl in config).",
					);
				}

				let cliHeaders: Record<string, string> | undefined;
				if (cliHeadersRaw) {
					try {
						cliHeaders = JSON.parse(cliHeadersRaw) as Record<string, string>;
					} catch (parseError) {
						throw new Error(
							`--headers must be valid JSON: ${
								parseError instanceof Error
									? parseError.message
									: String(parseError)
							}`,
						);
					}
				}

				const headers: Record<string, string> = {
					"Content-Type": "application/json",
					...configOptions.headers,
					...cliHeaders,
				};

				const fetcher = async (batch: FakeTimeSeriesData[]) => {
					return fetch(sinkUrl, {
						method: "POST",
						headers,
						body: JSON.stringify(batch),
					});
				};

				const parsedOptions: FakeTimeSeriesToSinkOptions = {
					...configOptions,
					...cliOptions,
					fetcher,
					// If the config file provides an onError handler, it wins.
					// Otherwise use the injectable `sinkBatchErrorHandler` so
					// tests can capture and assert on per-batch errors.
					onError: configOptions.onError ?? sinkBatchErrorHandler,
				};

				if (liveMode) {
					const intervalMs = readLiveIntervalMs(command);
					// Resolve startTime exactly once (see the generate action
					// handler for the full rationale — relative strings must
					// not drift across ticks).
					const initialStartTime = parseTime(
						parsedOptions.startTime ?? DEFAULT_START_TIME,
					);
					log("Sending data to sink in live mode...");
					await runLive({
						initialStartTime,
						intervalMs,
						signal: liveSignal,
						now: opts.now,
						sleep: opts.sleep,
						tick: async (windowStart, windowEnd) => {
							const tickOptions: FakeTimeSeriesToSinkOptions = {
								...parsedOptions,
								startTime: windowStart,
								endTime: windowEnd,
							};
							const result = await toSinkRunner(tickOptions);
							log(JSON.stringify(result, null, 2));
						},
					});
					return;
				}

				log("Sending data to sink...");
				const result = await toSinkRunner(parsedOptions);
				log(JSON.stringify(result, null, 2));
			} catch (error) {
				onError(error);
			}
		});

	return program;
}

/**
 * Detect whether this module is being run as the CLI entry point
 * (vs. imported by another module, e.g. a test). Uses basename matching
 * so an unrelated user script named `mycli.js` doesn't trigger false
 * positives via substring matches.
 */
const isDirectInvocation = (): boolean => {
	const entry = process.argv[1];
	if (!entry) {
		return false;
	}
	try {
		const resolved = fs.realpathSync(entry);
		const base = path.basename(resolved);
		return base === "cli.ts" || base === "cli.js" || base === "cli.mjs";
	} catch {
		return false;
	}
};

if (isDirectInvocation()) {
	// Wire SIGINT / SIGTERM to abort the live-mode tick loop so a Ctrl-C
	// exits cleanly instead of interrupting a tick mid-flight and leaving
	// a half-sent HTTP batch. We attach the listeners here (not inside
	// buildProgram) so the signal plumbing stays scoped to the actual
	// process and never pollutes tests or embedded consumers.
	const liveController = new AbortController();
	const onShutdownSignal = (): void => {
		liveController.abort();
	};
	const removeShutdownListeners = (): void => {
		process.removeListener("SIGINT", onShutdownSignal);
		process.removeListener("SIGTERM", onShutdownSignal);
	};
	process.on("SIGINT", onShutdownSignal);
	process.on("SIGTERM", onShutdownSignal);

	// Use try/catch to cover the (theoretical) path where `buildProgram`
	// or synchronous setup inside `parseAsync` throws before returning
	// the Promise — relying solely on `.finally()` would leak the signal
	// listeners for the lifetime of the process in that case.
	//
	// CRITICAL: `defaultErrorHandler` calls `process.exit(1)` synchronously,
	// which drops any pending microtasks — including this chain's own
	// `.finally`. So the `.catch` callback MUST remove the listeners
	// before invoking the error handler, otherwise the cleanup never
	// runs on the failure path. On the success path `.finally` still
	// handles cleanup, and `removeListener` is idempotent so doubling up
	// in edge cases is harmless.
	try {
		const program = buildProgram({ liveSignal: liveController.signal });
		program
			.parseAsync(process.argv)
			.catch((error) => {
				removeShutdownListeners();
				defaultErrorHandler(error);
			})
			.finally(removeShutdownListeners);
	} catch (error) {
		removeShutdownListeners();
		defaultErrorHandler(error);
	}
}
