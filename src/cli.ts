#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { Command } from "commander";
import {
	collectExplicitOptions,
	loadConfig,
	parsePositiveInt,
	parseProbability,
	readVersion,
} from "./cli-helpers";
import {
	type FakeTimeSeriesData,
	type FakeTimeSeriesOptions,
	type FakeTimeSeriesResult,
	type FakeTimeSeriesToSinkOptions,
	generate,
	toSink,
} from "./index";

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
		.option("-c, --config <path>", "Path to config file");

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

				// CLI-explicit options override config; config overrides library defaults.
				const parsedOptions: FakeTimeSeriesOptions = {
					...configOptions,
					...cliOptions,
				};

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
	const program = buildProgram();
	program.parseAsync(process.argv).catch((error) => {
		defaultErrorHandler(error);
	});
}
