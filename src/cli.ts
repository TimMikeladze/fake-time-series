#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { type Command, program } from "commander";
import {
	type FakeTimeSeriesData,
	type FakeTimeSeriesOptions,
	type FakeTimeSeriesToSinkOptions,
	generate,
	toSink,
} from ".";

async function loadConfig(configPath?: string): Promise<
	Partial<
		FakeTimeSeriesOptions &
			FakeTimeSeriesToSinkOptions & {
				headers?: Record<string, string>;
				sinkUrl?: string;
			}
	>
> {
	const configFile = configPath
		? path.resolve(process.cwd(), configPath)
		: path.resolve(process.cwd(), "fake-time-series.config.mjs");

	if (!fs.existsSync(configFile)) {
		// Try .mjs extension first
		const mjsFile = configFile.endsWith(".mjs")
			? configFile
			: `${configFile}.mjs`;

		if (fs.existsSync(mjsFile)) {
			const config = await import(mjsFile);
			return config.options;
		}

		// Try .js extension next
		const jsFile = configFile.replace(/\.mjs$/, ".js");
		if (fs.existsSync(jsFile)) {
			const config = await import(jsFile);
			return config.options;
		}

		throw new Error(
			`Config file not found: ${configFile} (tried .mjs and .js)`,
		);
	}

	const config = await import(configFile);
	return config.options;
}

program
	.name("fake-time-series")
	.description("CLI for generating and sending fake time-series data.")
	.version("1.0.0");

const withOptions = (command: Command) => {
	return command
		.option("-s, --startTime <date>", "Start time of the series", "-1 day")
		.option(
			"-e, --endTime <date>",
			"End time of the series",
			new Date().toISOString(),
		)
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
			"10",
		)
		.option("--no-batchSizeRandomization", "Disable batch size randomization")
		.option("--no-intervalRandomization", "Disable interval randomization")
		.option(
			"--batchReverseProbability <number>",
			"Probability to reverse each batch",
			"0.5",
		)
		.option(
			"--batchShuffleProbability <number>",
			"Probability to shuffle each batch",
			"0.4",
		)
		.option(
			"--intervalSkewProbability <number>",
			"Probability of interval skewing",
			"0.8",
		)
		.option("-c, --config <path>", "Path to config file");
};

const generateCommand = withOptions(program.command("generate"));

generateCommand
	.description("Generate fake time-series data")
	.action(async (options) => {
		const configOptions = await loadConfig(options.config);

		const parsedOptions: FakeTimeSeriesOptions = {
			...options,
			...configOptions,
		};

		const result = await generate(parsedOptions);
		console.log(JSON.stringify(result, null, 2));
	});

const sendCommand = withOptions(program.command("send"));

sendCommand
	.description("Generate fake time-series data and send it to a sink")
	.option(
		"--concurrency <number>",
		"Maximum number of concurrent requests",
		"10",
	)
	.option("--sink-url <url>", "URL of the sink to send data")
	.option(
		"--headers <json>",
		"JSON string of headers to include in each request",
	)
	.action(async (options) => {
		const configOptions = await loadConfig(options.config);

		if (!(options.sinkUrl || configOptions.sinkUrl)) {
			console.error("Sink URL is required.");
			process.exit(1);
		}

		const headers = options.headers
			? { ...configOptions.headers, ...JSON.parse(options.headers) }
			: configOptions.headers || { "Content-Type": "application/json" };

		const fetcher = async (batch: FakeTimeSeriesData[]) => {
			const response = await fetch(options.sinkUrl || configOptions.sinkUrl, {
				method: "POST",
				headers,
				body: JSON.stringify(batch),
			});
			return response;
		};

		const parsedOptions: FakeTimeSeriesToSinkOptions = {
			...options,
			...configOptions,
			fetcher,
			onError:
				configOptions.onError || ((error) => console.error("Error:", error)),
		};

		const result = await toSink(parsedOptions);
		console.log("Data sent to sink successfully:");
	});

program.parse(process.argv);
