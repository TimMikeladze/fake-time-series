import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { buildProgram } from "../src/cli";
import {
	loadConfig,
	parsePositiveInt,
	parseProbability,
	readVersion,
	resolveConfigPath,
} from "../src/cli-helpers";
import type {
	FakeTimeSeriesOptions,
	FakeTimeSeriesResult,
	FakeTimeSeriesToSinkOptions,
} from "../src/index";

const FIXTURE_CONFIG_PATH = path.resolve(
	__dirname,
	"fixtures/loadable-config.mjs",
);

/**
 * Run `fn` with `globalThis.fetch` temporarily replaced by `spy`, and
 * always restore the original — even if `fn` (or anything before the
 * inner call) throws. Safer than the open-coded `try/finally` pattern
 * that left the assignment outside the `try`.
 */
async function withFetchSpy<T>(
	spy: typeof fetch,
	fn: () => Promise<T>,
): Promise<T> {
	const originalFetch = globalThis.fetch;
	try {
		globalThis.fetch = spy;
		return await fn();
	} finally {
		globalThis.fetch = originalFetch;
	}
}

describe("resolveConfigPath", () => {
	let tmpDir: string;

	beforeEach(() => {
		tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "fts-cli-test-"));
	});

	afterEach(() => {
		fs.rmSync(tmpDir, { recursive: true, force: true });
	});

	it("returns null when no config path is given and no defaults exist", () => {
		// Previously the CLI threw "Config file not found" here and broke
		// for any user without a config file in cwd.
		expect(resolveConfigPath(undefined, tmpDir)).toBeNull();
	});

	it("finds the .mjs default config", () => {
		const file = path.join(tmpDir, "fake-time-series.config.mjs");
		fs.writeFileSync(file, "export const options = { maxBatchSize: 42 };\n");

		expect(resolveConfigPath(undefined, tmpDir)).toBe(file);
	});

	it("falls back to the .js default config when .mjs is absent", () => {
		const file = path.join(tmpDir, "fake-time-series.config.js");
		fs.writeFileSync(file, "export const options = { maxBatchSize: 7 };\n");

		expect(resolveConfigPath(undefined, tmpDir)).toBe(file);
	});

	it("prefers the .mjs default when both exist", () => {
		const mjs = path.join(tmpDir, "fake-time-series.config.mjs");
		const js = path.join(tmpDir, "fake-time-series.config.js");
		fs.writeFileSync(mjs, "export const options = {};\n");
		fs.writeFileSync(js, "export const options = {};\n");

		expect(resolveConfigPath(undefined, tmpDir)).toBe(mjs);
	});

	it("throws when an explicit config path does not exist", () => {
		expect(() => resolveConfigPath("nope.config.mjs", tmpDir)).toThrow(
			/Config file not found/,
		);
	});

	it("resolves an explicit config path with extension", () => {
		const file = path.join(tmpDir, "custom.mjs");
		fs.writeFileSync(file, "export const options = {};\n");

		expect(resolveConfigPath("custom.mjs", tmpDir)).toBe(file);
	});

	it("tries .mjs and .js extensions when none is given on explicit path", () => {
		const file = path.join(tmpDir, "custom.mjs");
		fs.writeFileSync(file, "export const options = {};\n");

		expect(resolveConfigPath("custom", tmpDir)).toBe(file);
	});

	it("falls back to .js for explicit path with no extension when .mjs missing", () => {
		const file = path.join(tmpDir, "custom.js");
		fs.writeFileSync(file, "export const options = {};\n");

		expect(resolveConfigPath("custom", tmpDir)).toBe(file);
	});

	it("error message lists all candidates tried", () => {
		expect(() => resolveConfigPath("custom", tmpDir)).toThrow(
			/tried .*custom\.mjs.*custom\.js/,
		);
	});
});

describe("loadConfig", () => {
	it("returns empty config when no config file exists and none is requested", async () => {
		const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "fts-cli-test-"));
		const originalCwd = process.cwd();
		try {
			process.chdir(tmpDir);
			const config = await loadConfig();
			expect(config).toEqual({});
		} finally {
			process.chdir(originalCwd);
			fs.rmSync(tmpDir, { recursive: true, force: true });
		}
	});

	it("loads options from an explicit fixture config", async () => {
		const config = await loadConfig(FIXTURE_CONFIG_PATH);
		expect(config).toMatchObject({
			maxBatchSize: 7,
			batchReverseProbability: 0.25,
			concurrency: 3,
		});
	});
});

describe("parsePositiveInt", () => {
	const parse = parsePositiveInt("--count");

	it("parses a valid positive integer", () => {
		expect(parse("42")).toBe(42);
	});

	it("accepts integer values written with a trailing .0", () => {
		// "4.0" is a valid way to write 4; previously this threw because
		// Number.parseInt("4.0") produced 4 and a strict string match
		// rejected it. The new parseFloat+isInteger path accepts it.
		expect(parse("4.0")).toBe(4);
	});

	it("trims surrounding whitespace", () => {
		expect(parse("  42  ")).toBe(42);
	});

	it("rejects zero", () => {
		expect(() => parse("0")).toThrow(/--count must be a positive integer/);
	});

	it("rejects negative", () => {
		expect(() => parse("-3")).toThrow(/--count must be a positive integer/);
	});

	it("rejects non-numeric", () => {
		expect(() => parse("abc")).toThrow(/--count must be a positive integer/);
	});

	it("rejects decimals", () => {
		expect(() => parse("3.5")).toThrow(/--count must be a positive integer/);
	});

	it("rejects trailing garbage", () => {
		// "42abc" must be rejected — accepting a partial parse here would
		// silently corrupt user input. The regex-gated parser catches it
		// before parseFloat gets a chance to drop the "abc" suffix.
		expect(() => parse("42abc")).toThrow(/--count must be a positive integer/);
	});

	it("rejects scientific notation", () => {
		// "4e2" might mean 400 under parseFloat, but CLI users shouldn't
		// be expressing batch sizes in scientific notation.
		expect(() => parse("4e2")).toThrow(/--count must be a positive integer/);
	});

	it("rejects Infinity", () => {
		expect(() => parse("Infinity")).toThrow(
			/--count must be a positive integer/,
		);
	});

	it("rejects leading plus sign", () => {
		// "+5" is technically a positive integer under parseFloat but
		// the regex is stricter — users should type "5" not "+5".
		expect(() => parse("+5")).toThrow(/--count must be a positive integer/);
	});
});

describe("parseProbability", () => {
	const parse = parseProbability("--p");

	it("parses 0 and 1 inclusively", () => {
		expect(parse("0")).toBe(0);
		expect(parse("1")).toBe(1);
	});

	it("parses fractional values", () => {
		expect(parse("0.5")).toBe(0.5);
		expect(parse("0.123")).toBeCloseTo(0.123);
	});

	it("rejects values below 0", () => {
		expect(() => parse("-0.1")).toThrow(/--p must be a number between 0 and 1/);
	});

	it("rejects values above 1", () => {
		expect(() => parse("1.5")).toThrow(/--p must be a number between 0 and 1/);
	});

	it("rejects non-numeric", () => {
		expect(() => parse("nope")).toThrow(/--p must be a number between 0 and 1/);
	});

	it("rejects partial parses (e.g. '0.5 xyz')", () => {
		// parseFloat would silently accept "0.5 xyz" as 0.5; the regex
		// gate catches it before that can happen.
		expect(() => parse("0.5 xyz")).toThrow(
			/--p must be a number between 0 and 1/,
		);
	});

	it("rejects scientific notation", () => {
		expect(() => parse("5e-1")).toThrow(/--p must be a number between 0 and 1/);
	});

	it("rejects leading sign", () => {
		expect(() => parse("+0.5")).toThrow(/--p must be a number between 0 and 1/);
		expect(() => parse("-0.5")).toThrow(/--p must be a number between 0 and 1/);
	});
});

describe("readVersion", () => {
	it("returns a string version", () => {
		const v = readVersion();
		expect(typeof v).toBe("string");
		// Either a semver-ish value from package.json, or the "unknown" fallback.
		expect(v === "unknown" || /^\d+\.\d+\.\d+/.test(v)).toBe(true);
	});
});

/**
 * End-to-end CLI tests exercise the full commander action handlers
 * (option parsing, config merge, precedence, error handling) by driving
 * `buildProgram` with stub runners. No subprocess needed.
 */
describe("buildProgram", () => {
	const makeHarness = () => {
		const generateCalls: FakeTimeSeriesOptions[] = [];
		const toSinkCalls: FakeTimeSeriesToSinkOptions[] = [];
		const logs: string[] = [];
		const errors: unknown[] = [];

		const stubResult = (): FakeTimeSeriesResult => ({
			batches: [],
			startTime: new Date("2024-01-01T00:00:00Z"),
			endTime: new Date("2024-01-01T00:01:00Z"),
			minInterval: 1000,
			maxInterval: 1000,
			totalBatches: 0,
			totalMessages: 0,
		});

		const program = buildProgram({
			generateRunner: async (options) => {
				generateCalls.push(options);
				return stubResult();
			},
			toSinkRunner: async (options) => {
				toSinkCalls.push(options);
				return stubResult();
			},
			onError: (error) => {
				errors.push(error);
				throw error;
			},
			log: (msg) => {
				logs.push(msg);
			},
		});
		// Commander normally calls process.exit on help/version/errors.
		// Override so it throws and our test can catch.
		program.exitOverride();
		for (const cmd of program.commands) {
			cmd.exitOverride();
		}

		const run = (argv: string[]) =>
			program.parseAsync(["node", "cli", ...argv], { from: "node" });

		return { program, run, generateCalls, toSinkCalls, logs, errors };
	};

	describe("generate command", () => {
		it("passes CLI-explicit options through to the runner", async () => {
			const h = makeHarness();
			await h.run([
				"generate",
				"--startTime",
				"2024-01-01T00:00:00Z",
				"--endTime",
				"2024-01-01T00:01:00Z",
				"--minInterval",
				"10s",
				"--maxInterval",
				"10s",
				"--maxBatchSize",
				"5",
			]);
			expect(h.generateCalls).toHaveLength(1);
			expect(h.generateCalls[0]).toMatchObject({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:01:00Z",
				minInterval: "10s",
				maxInterval: "10s",
				maxBatchSize: 5,
			});
		});

		it("parses --maxBatchSize into a number, not a string", async () => {
			const h = makeHarness();
			await h.run(["generate", "--maxBatchSize", "7"]);
			expect(h.generateCalls[0].maxBatchSize).toBe(7);
			expect(typeof h.generateCalls[0].maxBatchSize).toBe("number");
		});

		it("rejects invalid --maxBatchSize at parse time", async () => {
			const h = makeHarness();
			await expect(
				h.run(["generate", "--maxBatchSize", "abc"]),
			).rejects.toThrow(/--maxBatchSize must be a positive integer/);
			expect(h.generateCalls).toHaveLength(0);
		});

		it("parses probability flags as numbers", async () => {
			const h = makeHarness();
			await h.run([
				"generate",
				"--batchReverseProbability",
				"0.1",
				"--batchShuffleProbability",
				"0.2",
				"--intervalSkewProbability",
				"0.3",
			]);
			expect(h.generateCalls[0]).toMatchObject({
				batchReverseProbability: 0.1,
				batchShuffleProbability: 0.2,
				intervalSkewProbability: 0.3,
			});
		});

		it("rejects out-of-range probability at parse time", async () => {
			const h = makeHarness();
			await expect(
				h.run(["generate", "--batchReverseProbability", "1.5"]),
			).rejects.toThrow(/must be a number between 0 and 1/);
		});

		it("--no-batchSizeRandomization propagates as false", async () => {
			const h = makeHarness();
			await h.run(["generate", "--no-batchSizeRandomization"]);
			expect(h.generateCalls[0].batchSizeRandomization).toBe(false);
		});

		it("loads options from --config file and merges with CLI flags", async () => {
			const h = makeHarness();
			await h.run([
				"generate",
				"--config",
				FIXTURE_CONFIG_PATH,
				"--maxBatchSize",
				"99",
			]);
			// CLI flag must win over config
			expect(h.generateCalls[0].maxBatchSize).toBe(99);
			// Config value should be present where no CLI flag was passed
			expect(h.generateCalls[0].batchReverseProbability).toBe(0.25);
		});

		it("uses config value when CLI flag is not passed", async () => {
			const h = makeHarness();
			await h.run(["generate", "--config", FIXTURE_CONFIG_PATH]);
			expect(h.generateCalls[0].maxBatchSize).toBe(7);
			expect(h.generateCalls[0].batchReverseProbability).toBe(0.25);
		});

		it("does not require a config file to exist when --config is absent", async () => {
			const h = makeHarness();
			const originalCwd = process.cwd();
			const tmpDir = fs.mkdtempSync(
				path.join(os.tmpdir(), "fts-cli-noconfig-"),
			);
			try {
				process.chdir(tmpDir);
				// Previously this threw "Config file not found"; now it should work.
				await h.run(["generate", "--maxBatchSize", "3"]);
				expect(h.generateCalls[0].maxBatchSize).toBe(3);
			} finally {
				process.chdir(originalCwd);
				fs.rmSync(tmpDir, { recursive: true, force: true });
			}
		});

		it("does not include the --config attribute in the library options", async () => {
			const h = makeHarness();
			await h.run([
				"generate",
				"--config",
				FIXTURE_CONFIG_PATH,
				"--maxBatchSize",
				"2",
			]);
			expect(
				(h.generateCalls[0] as unknown as Record<string, unknown>).config,
			).toBeUndefined();
		});

		it("routes runner errors through the onError hook", async () => {
			const errors: unknown[] = [];
			const program = buildProgram({
				generateRunner: async () => {
					throw new Error("library boom");
				},
				onError: (err) => {
					errors.push(err);
					throw err;
				},
				log: () => undefined,
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}
			await expect(
				program.parseAsync(["node", "cli", "generate"], { from: "node" }),
			).rejects.toThrow("library boom");
			expect(errors).toHaveLength(1);
		});

		it("end-to-end: generate action with real library produces output", async () => {
			// No generateRunner stub — this test exercises the full wiring
			// from commander → action handler → real generate() → JSON output.
			// Catches regressions where the merge/spread logic silently drops
			// or mangles an option on the way to the library.
			const logs: string[] = [];
			const program = buildProgram({
				onError: (err) => {
					throw err;
				},
				log: (msg) => {
					logs.push(msg);
				},
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}

			// Use a tmp cwd so the repo's own fake-time-series.config.mjs
			// doesn't get auto-loaded and alter the output.
			const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "fts-cli-e2e-"));
			const originalCwd = process.cwd();
			try {
				process.chdir(tmpDir);
				await program.parseAsync(
					[
						"node",
						"cli",
						"generate",
						"--startTime",
						"2024-01-01T00:00:00Z",
						"--endTime",
						"2024-01-01T00:00:20Z",
						"--minInterval",
						"5s",
						"--maxInterval",
						"5s",
						"--no-batchSizeRandomization",
						"--maxBatchSize",
						"2",
					],
					{ from: "node" },
				);
			} finally {
				process.chdir(originalCwd);
				fs.rmSync(tmpDir, { recursive: true, force: true });
			}

			expect(logs).toHaveLength(1);
			const output = JSON.parse(logs[0]) as {
				totalMessages: number;
				totalBatches: number;
				batches: Array<Array<{ timestamp: number; key: string }>>;
			};
			expect(output.totalMessages).toBeGreaterThan(0);
			expect(output.totalBatches).toBeGreaterThan(0);
			// Every datapoint should have the expected shape from the default
			// shapes map (since cwd has no config and no --shapes flag).
			for (const batch of output.batches) {
				for (const dp of batch) {
					expect(typeof dp.timestamp).toBe("number");
					expect(dp.key).toBe("default");
				}
			}
		});
	});

	describe("send command", () => {
		it("requires a sink URL (errors when absent)", async () => {
			const h = makeHarness();
			await expect(h.run(["send"])).rejects.toThrow(/Sink URL is required/);
			expect(h.toSinkCalls).toHaveLength(0);
		});

		it("uses --sink-url from CLI", async () => {
			const h = makeHarness();
			await h.run(["send", "--sink-url", "https://example.com/ingest"]);
			expect(h.toSinkCalls).toHaveLength(1);
			const call = h.toSinkCalls[0];
			expect(typeof call.fetcher).toBe("function");
			// sinkUrl itself is not part of FakeTimeSeriesToSinkOptions — it's
			// consumed by the CLI to construct the fetcher — so it should NOT
			// be spread into the library options.
			expect(
				(call as unknown as Record<string, unknown>).sinkUrl,
			).toBeUndefined();
		});

		it("parses --concurrency as a number", async () => {
			const h = makeHarness();
			await h.run([
				"send",
				"--sink-url",
				"https://example.com",
				"--concurrency",
				"5",
			]);
			expect(h.toSinkCalls[0].concurrency).toBe(5);
		});

		it("parses --headers as JSON and merges with defaults", async () => {
			const h = makeHarness();
			await h.run([
				"send",
				"--sink-url",
				"https://example.com",
				"--headers",
				'{"X-Test":"yes"}',
			]);
			// The fetcher closure captured the parsed headers; we can verify
			// indirectly by invoking it and inspecting fetch arguments.
			const fetchSpy = vi.fn(async () => new Response("", { status: 200 }));
			await withFetchSpy(fetchSpy as unknown as typeof fetch, async () => {
				await h.toSinkCalls[0].fetcher([{ timestamp: 0, key: "k", data: {} }]);
			});
			expect(fetchSpy).toHaveBeenCalledWith(
				"https://example.com",
				expect.objectContaining({
					method: "POST",
					headers: expect.objectContaining({
						"Content-Type": "application/json",
						"X-Test": "yes",
					}),
				}),
			);
		});

		it("rejects invalid --headers JSON with a helpful message", async () => {
			const h = makeHarness();
			await expect(
				h.run([
					"send",
					"--sink-url",
					"https://example.com",
					"--headers",
					"not-json",
				]),
			).rejects.toThrow(/--headers must be valid JSON/);
		});

		it("prefers --sink-url over config sinkUrl", async () => {
			const h = makeHarness();
			// The fixture sets sinkUrl: "https://config.example.com/ingest".
			// Pass a DIFFERENT --sink-url on the CLI and verify the fetcher
			// uses the CLI value, not the config value. This is a real
			// precedence test (earlier iterations had a fixture with no
			// sinkUrl, making this assertion vacuous).
			await h.run([
				"send",
				"--config",
				FIXTURE_CONFIG_PATH,
				"--sink-url",
				"https://override.example.com",
			]);
			const fetchSpy = vi.fn(async () => new Response("", { status: 200 }));
			await withFetchSpy(fetchSpy as unknown as typeof fetch, async () => {
				await h.toSinkCalls[0].fetcher([{ timestamp: 0, key: "k", data: {} }]);
			});
			expect(fetchSpy).toHaveBeenCalledWith(
				"https://override.example.com",
				expect.anything(),
			);
			// And NOT the config URL.
			expect(fetchSpy).not.toHaveBeenCalledWith(
				"https://config.example.com/ingest",
				expect.anything(),
			);
		});

		it("falls back to config sinkUrl when --sink-url is absent", async () => {
			const h = makeHarness();
			await h.run(["send", "--config", FIXTURE_CONFIG_PATH]);
			const fetchSpy = vi.fn(async () => new Response("", { status: 200 }));
			await withFetchSpy(fetchSpy as unknown as typeof fetch, async () => {
				await h.toSinkCalls[0].fetcher([{ timestamp: 0, key: "k", data: {} }]);
			});
			expect(fetchSpy).toHaveBeenCalledWith(
				"https://config.example.com/ingest",
				expect.anything(),
			);
		});

		it("merges config headers with CLI headers (CLI wins)", async () => {
			const h = makeHarness();
			await h.run([
				"send",
				"--config",
				FIXTURE_CONFIG_PATH,
				"--sink-url",
				"https://example.com",
				"--headers",
				'{"X-Config-Header":"from-cli","X-Cli-Only":"yes"}',
			]);
			const fetchSpy = vi.fn(async () => new Response("", { status: 200 }));
			await withFetchSpy(fetchSpy as unknown as typeof fetch, async () => {
				await h.toSinkCalls[0].fetcher([{ timestamp: 0, key: "k", data: {} }]);
			});
			expect(fetchSpy).toHaveBeenCalledWith(
				"https://example.com",
				expect.objectContaining({
					headers: expect.objectContaining({
						"Content-Type": "application/json",
						// CLI value wins for the overlapping key
						"X-Config-Header": "from-cli",
						// CLI-only key makes it through
						"X-Cli-Only": "yes",
					}),
				}),
			);
		});

		it("uses config headers when no --headers flag is passed", async () => {
			const h = makeHarness();
			await h.run([
				"send",
				"--config",
				FIXTURE_CONFIG_PATH,
				"--sink-url",
				"https://example.com",
			]);
			const fetchSpy = vi.fn(async () => new Response("", { status: 200 }));
			await withFetchSpy(fetchSpy as unknown as typeof fetch, async () => {
				await h.toSinkCalls[0].fetcher([{ timestamp: 0, key: "k", data: {} }]);
			});
			expect(fetchSpy).toHaveBeenCalledWith(
				"https://example.com",
				expect.objectContaining({
					headers: expect.objectContaining({
						"X-Config-Header": "from-config",
					}),
				}),
			);
		});

		it("merges config options with CLI flags (CLI wins)", async () => {
			const h = makeHarness();
			await h.run([
				"send",
				"--sink-url",
				"https://example.com",
				"--config",
				FIXTURE_CONFIG_PATH,
				"--concurrency",
				"11",
			]);
			const call = h.toSinkCalls[0];
			// CLI wins: concurrency 11 over config's 3
			expect(call.concurrency).toBe(11);
			// Config wins where no CLI flag: maxBatchSize 7 from fixture
			expect(call.maxBatchSize).toBe(7);
		});

		it("passes injected sinkBatchErrorHandler through to toSink", async () => {
			// Drive the real toSink path (no toSinkRunner stub) with a
			// fetcher that always fails, and assert the injected handler
			// receives every error. This closes the wiring gap where the
			// per-batch error handler used to be hardcoded and untestable.
			const batchErrors: unknown[] = [];
			const failingFetch = (async () =>
				new Response("boom", { status: 500 })) as typeof fetch;

			await withFetchSpy(failingFetch, async () => {
				const program = buildProgram({
					sinkBatchErrorHandler: (err) => {
						batchErrors.push(err);
					},
					onError: (err) => {
						throw err;
					},
					log: () => undefined,
				});
				program.exitOverride();
				for (const cmd of program.commands) {
					cmd.exitOverride();
				}
				await program.parseAsync(
					[
						"node",
						"cli",
						"send",
						"--sink-url",
						"https://example.com",
						"--startTime",
						"2024-01-01T00:00:00Z",
						"--endTime",
						"2024-01-01T00:00:30Z",
						"--minInterval",
						"10s",
						"--maxInterval",
						"10s",
						"--no-batchSizeRandomization",
						"--maxBatchSize",
						"1",
					],
					{ from: "node" },
				);
			});

			// Every batch should have produced an error routed through our
			// handler (proving the injection wiring works end-to-end).
			expect(batchErrors.length).toBeGreaterThan(0);
			for (const err of batchErrors) {
				expect(err).toBeInstanceOf(Error);
				expect((err as Error).message).toContain("500");
			}
		});
	});
});
