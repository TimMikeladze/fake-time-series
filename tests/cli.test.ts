import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { buildProgram } from "../src/cli";
import {
	loadConfig,
	parseDurationMs,
	parsePositiveInt,
	parseProbability,
	readLiveIntervalMs,
	readVersion,
	resolveConfigPath,
	runLive,
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

describe("parseDurationMs", () => {
	const parse = parseDurationMs("--live-interval");

	it("parses a positive millisecond integer", () => {
		expect(parse("500")).toBe(500);
	});

	it("parses human-readable seconds via ms()", () => {
		expect(parse("1s")).toBe(1000);
	});

	it("parses human-readable milliseconds via ms()", () => {
		expect(parse("250ms")).toBe(250);
	});

	it("parses minutes", () => {
		expect(parse("2m")).toBe(120_000);
	});

	it("accepts zero as a valid (instant re-tick) interval", () => {
		// A 0ms live-interval is legal: it means "sleep 0, then immediately
		// tick again". Rejecting 0 would force users to pick a surprising
		// floor like 1ms; the tests below that use `--live-interval 0` in
		// CLI integration rely on this behavior.
		expect(parse("0")).toBe(0);
	});

	it("rejects negative millisecond integers", () => {
		expect(() => parse("-1")).toThrow(/--live-interval/);
	});

	it("rejects negative human-readable durations", () => {
		expect(() => parse("-1s")).toThrow(/--live-interval/);
	});

	it("rejects garbage strings", () => {
		expect(() => parse("nope")).toThrow(/--live-interval/);
	});

	it("rejects partial parses", () => {
		// "1s foo" must not silently parse as 1000 — catches the same class
		// of bug parsePositiveInt guards against with its regex gate.
		expect(() => parse("1s foo")).toThrow(/--live-interval/);
	});

	it("rejects interior whitespace even when ms() would accept it", () => {
		// `ms("1 s")` returns 1000 — the library happily accepts an
		// interior space between the number and the unit. We intentionally
		// reject this at the CLI boundary because:
		//   1. Users writing duration flags should use the canonical form
		//      ("1s", not "1 s"); permitting both is needless variation.
		//   2. Accepting interior whitespace widens the attack surface for
		//      partial-parse bugs if a future `ms` release starts
		//      accepting "1 s foo".
		expect(() => parse("1 s")).toThrow(/--live-interval/);
	});

	it("rejects an empty string with the caller's friendly error", () => {
		// `ms("")` throws a NATIVE error ("val is not a non-empty string
		// or a valid number"), not returns undefined — so without an
		// explicit early guard, the user would see the ms library's
		// internal message leaking through instead of the friendly
		// `--live-interval must be a non-negative duration...` message.
		expect(() => parse("")).toThrow(/--live-interval/);
	});

	it("rejects whitespace-only input (collapses to empty after trim)", () => {
		// After `.trim()` both "   " and "\t" become "", which would
		// otherwise fall through to the same `ms("")` throw path.
		expect(() => parse("   ")).toThrow(/--live-interval/);
		expect(() => parse("\t")).toThrow(/--live-interval/);
	});
});

describe("readLiveIntervalMs", () => {
	// Minimal command-shaped stub: only `getOptionValue` is consulted.
	const fakeCommand = (
		value: unknown,
	): Parameters<typeof readLiveIntervalMs>[0] =>
		({
			getOptionValue: (_name: string) => value,
		}) as unknown as Parameters<typeof readLiveIntervalMs>[0];

	it("returns the number as-is when it is a finite non-negative number", () => {
		expect(readLiveIntervalMs(fakeCommand(1000))).toBe(1000);
		expect(readLiveIntervalMs(fakeCommand(0))).toBe(0);
		expect(readLiveIntervalMs(fakeCommand(0.5))).toBe(0.5);
	});

	it("throws an internal-error message on a non-number (wiring bug guard)", () => {
		// This path only fires if commander's parser wiring is broken —
		// e.g. a future refactor detaches parseDurationMs from the option.
		// The message must clearly say "Internal error" so the reader
		// understands it's NOT a user-facing validation failure.
		expect(() => readLiveIntervalMs(fakeCommand("1000"))).toThrow(
			/Internal error/,
		);
		expect(() => readLiveIntervalMs(fakeCommand(undefined))).toThrow(
			/Internal error/,
		);
		expect(() => readLiveIntervalMs(fakeCommand(null))).toThrow(
			/Internal error/,
		);
	});

	it("throws on a negative number", () => {
		expect(() => readLiveIntervalMs(fakeCommand(-1))).toThrow(/Internal error/);
	});

	it("throws on NaN or Infinity", () => {
		expect(() => readLiveIntervalMs(fakeCommand(Number.NaN))).toThrow(
			/Internal error/,
		);
		expect(() =>
			readLiveIntervalMs(fakeCommand(Number.POSITIVE_INFINITY)),
		).toThrow(/Internal error/);
	});
});

describe("runLive", () => {
	it("invokes the tick function with advancing windows where each start equals the previous end", async () => {
		const ticks: Array<{ start: Date; end: Date }> = [];
		const controller = new AbortController();
		let clockMs = new Date("2024-01-01T00:00:00Z").getTime();

		await runLive({
			initialStartTime: new Date("2024-01-01T00:00:00Z"),
			intervalMs: 100,
			signal: controller.signal,
			now: () => new Date(clockMs),
			sleep: async (ms) => {
				clockMs += ms;
			},
			tick: async (start, end) => {
				ticks.push({ start, end });
				// Simulate the tick doing a tiny bit of work so the next
				// window is guaranteed to be non-empty.
				clockMs += 5;
				if (ticks.length >= 3) {
					controller.abort();
				}
			},
		});

		expect(ticks).toHaveLength(3);
		// First tick starts exactly at the user-supplied initialStartTime.
		expect(ticks[0].start.toISOString()).toBe("2024-01-01T00:00:00.000Z");
		// Every subsequent tick's start equals the previous tick's end —
		// this is the core contract that makes live mode non-lossy (no gap
		// between windows) and non-overlapping (no duplicate data).
		for (let i = 1; i < ticks.length; i++) {
			expect(ticks[i].start.getTime()).toBe(ticks[i - 1].end.getTime());
		}
		// Every window is non-empty.
		for (const t of ticks) {
			expect(t.end.getTime()).toBeGreaterThan(t.start.getTime());
		}
	});

	it("exits immediately without ticking when signal is already aborted", async () => {
		const controller = new AbortController();
		controller.abort();
		let ticks = 0;

		await runLive({
			initialStartTime: new Date("2024-01-01T00:00:00Z"),
			intervalMs: 100,
			signal: controller.signal,
			now: () => new Date("2024-01-01T00:00:05Z"),
			sleep: async () => undefined,
			tick: async () => {
				ticks++;
			},
		});

		expect(ticks).toBe(0);
	});

	it("skips tick when the window is empty but keeps sleeping until aborted", async () => {
		let tickCalls = 0;
		let sleepCalls = 0;
		const controller = new AbortController();
		// Clock never advances — every window would be [now, now] (empty).
		const frozen = new Date("2024-01-01T00:00:00Z");

		await runLive({
			initialStartTime: frozen,
			intervalMs: 1,
			signal: controller.signal,
			now: () => frozen,
			sleep: async () => {
				sleepCalls++;
				if (sleepCalls >= 3) {
					controller.abort();
				}
			},
			tick: async () => {
				tickCalls++;
			},
		});

		// Tick never fired because every window was empty (start == end).
		// Previously, a naïve implementation would have thrown "Start time
		// must be before end time" from the library on the very first call.
		expect(tickCalls).toBe(0);
		// The loop kept running and sleeping — so it's still alive and
		// respecting the abort signal, just not producing data.
		expect(sleepCalls).toBeGreaterThanOrEqual(3);
	});

	it("propagates errors thrown by the tick function", async () => {
		const controller = new AbortController();
		await expect(
			runLive({
				initialStartTime: new Date("2024-01-01T00:00:00Z"),
				intervalMs: 100,
				signal: controller.signal,
				now: () => new Date("2024-01-01T00:00:05Z"),
				sleep: async () => undefined,
				tick: async () => {
					throw new Error("tick kaboom");
				},
			}),
		).rejects.toThrow("tick kaboom");
	});

	it("rejects a negative intervalMs", async () => {
		const controller = new AbortController();
		await expect(
			runLive({
				initialStartTime: new Date(),
				intervalMs: -1,
				signal: controller.signal,
				tick: async () => undefined,
			}),
		).rejects.toThrow(/intervalMs/);
	});

	it("rejects a non-finite intervalMs", async () => {
		const controller = new AbortController();
		await expect(
			runLive({
				initialStartTime: new Date(),
				intervalMs: Number.POSITIVE_INFINITY,
				signal: controller.signal,
				tick: async () => undefined,
			}),
		).rejects.toThrow(/intervalMs/);
	});

	it("calls sleep between ticks with the requested intervalMs", async () => {
		const sleeps: number[] = [];
		const controller = new AbortController();
		let clockMs = new Date("2024-01-01T00:00:00Z").getTime();
		let tickCount = 0;

		await runLive({
			initialStartTime: new Date("2024-01-01T00:00:00Z"),
			intervalMs: 250,
			signal: controller.signal,
			now: () => new Date(clockMs),
			sleep: async (ms) => {
				sleeps.push(ms);
				clockMs += ms;
			},
			tick: async () => {
				tickCount++;
				// Nudge the clock forward so the next window is non-empty.
				clockMs += 1;
				if (tickCount >= 3) {
					controller.abort();
				}
			},
		});

		expect(sleeps.length).toBeGreaterThan(0);
		for (const s of sleeps) {
			expect(s).toBe(250);
		}
	});

	it("uses the real default sleep and wakes cleanly when the signal fires mid-sleep", async () => {
		// Every other runLive test injects a no-op `sleep`, so the real
		// `timers/promises#setTimeout` path — the only path that exercises
		// the DOMException-safe AbortError swallow in `defaultLiveSleep` —
		// was untested before this case. We use a deliberately LONG sleep
		// (10 seconds) and then abort from the outside after 50ms; if
		// the AbortError weren't swallowed the promise would reject, and
		// if the abort weren't wired into the timer we'd wait out the
		// full 10 seconds. Either failure mode would blow this test up.
		const controller = new AbortController();
		let ticks = 0;
		const startedAt = Date.now();

		// Initial window is 1 second behind "now" so the first tick
		// fires immediately, then the loop enters the real sleep.
		const runPromise = runLive({
			initialStartTime: new Date(startedAt - 1000),
			intervalMs: 10_000,
			signal: controller.signal,
			// No `sleep` override on purpose — exercises defaultLiveSleep.
			tick: async () => {
				ticks++;
			},
		});

		// Give the loop a moment to complete the first tick and enter
		// the real sleep, then abort.
		await new Promise((resolve) => setTimeout(resolve, 50));
		controller.abort();

		// Must resolve cleanly (NOT reject with AbortError). If the
		// swallow path is broken this `await` rejects and the test fails.
		await runPromise;

		const elapsed = Date.now() - startedAt;
		expect(ticks).toBe(1);
		// We waited 50ms + a tiny bit of teardown — nowhere near the
		// configured 10s interval, which proves the abort actually
		// interrupted the sleep instead of letting it run to completion.
		expect(elapsed).toBeLessThan(2_000);
	});

	it("does not sleep after a tick if the signal was aborted during that tick", async () => {
		const sleeps: number[] = [];
		const controller = new AbortController();
		const initialStart = new Date("2024-01-01T00:00:00Z");
		// Clock starts AHEAD of initialStart so the very first window is
		// non-empty and the tick actually fires on iteration 1. Without
		// this primer, the loop would skip the tick (empty window) and
		// sleep forever.
		let clockMs = initialStart.getTime() + 1000;
		let tickCount = 0;

		await runLive({
			initialStartTime: initialStart,
			intervalMs: 500,
			signal: controller.signal,
			now: () => new Date(clockMs),
			sleep: async (intervalMs) => {
				sleeps.push(intervalMs);
			},
			tick: async () => {
				tickCount++;
				clockMs += 10;
				// Abort *during* the tick — the loop should check the signal
				// before sleeping and exit without paying the sleep cost.
				controller.abort();
			},
		});

		expect(tickCount).toBe(1);
		expect(sleeps).toHaveLength(0);
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

		it("loops in live mode, logs once per tick, and exits when aborted", async () => {
			// End-to-end live mode for `generate`:
			//   * runner fires once per tick
			//   * each tick's window chains cleanly from the previous
			//     (start == previous end)
			//   * the first window's start equals the user-supplied startTime,
			//     resolved ONCE up front (not re-evaluated per tick, which
			//     would drift on relative strings like "-1 day")
			//   * one log per tick
			//   * loop exits after the controller aborts (no hang)
			const generateCalls: FakeTimeSeriesOptions[] = [];
			const logs: string[] = [];
			const controller = new AbortController();
			let clockMs = new Date("2024-01-01T00:00:00Z").getTime();

			const program = buildProgram({
				generateRunner: async (options) => {
					generateCalls.push(options);
					// Nudge the fake clock so the next window is non-empty.
					clockMs += 1;
					if (generateCalls.length >= 3) {
						controller.abort();
					}
					return {
						batches: [],
						startTime: options.startTime as Date,
						endTime: options.endTime as Date,
						minInterval: 1000,
						maxInterval: 1000,
						totalBatches: 0,
						totalMessages: 0,
					};
				},
				log: (msg) => {
					logs.push(msg);
				},
				onError: (err) => {
					throw err;
				},
				liveSignal: controller.signal,
				now: () => new Date(clockMs),
				sleep: async (ms) => {
					clockMs += ms;
				},
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}

			await program.parseAsync(
				[
					"node",
					"cli",
					"generate",
					"--startTime",
					"2024-01-01T00:00:00Z",
					"--minInterval",
					"1s",
					"--maxInterval",
					"1s",
					"--live",
					"--live-interval",
					"100",
				],
				{ from: "node" },
			);

			expect(generateCalls).toHaveLength(3);
			// First tick's start == resolved user startTime.
			expect((generateCalls[0].startTime as Date).toISOString()).toBe(
				"2024-01-01T00:00:00.000Z",
			);
			// Every tick's start == previous tick's end (no gap/overlap).
			for (let i = 1; i < generateCalls.length; i++) {
				expect((generateCalls[i].startTime as Date).getTime()).toBe(
					(generateCalls[i - 1].endTime as Date).getTime(),
				);
			}
			// Each tick logs its own JSON result.
			expect(logs).toHaveLength(3);
			for (const msg of logs) {
				// Must be valid JSON so live output is still machine-pipeable.
				expect(() => JSON.parse(msg)).not.toThrow();
			}
			// CLI-only live flags must not leak into library options.
			for (const call of generateCalls) {
				const raw = call as unknown as Record<string, unknown>;
				expect(raw.live).toBeUndefined();
				expect(raw.liveInterval).toBeUndefined();
				expect(raw.config).toBeUndefined();
			}
		});

		it("rejects --endTime + --live with a clear incompatibility message", async () => {
			// Without this guard, a user who writes
			// `generate --endTime 2026-01-01 --live` would silently have
			// their endTime overwritten by `windowEnd = now()` on every
			// tick — a data-loss-adjacent footgun (the user thinks they
			// asked for bounded streaming, but the loop streams
			// indefinitely). The error keeps the two flags in opposing
			// corners so the mistake is surfaced loudly at parse time.
			const controller = new AbortController();
			const generateCalls: FakeTimeSeriesOptions[] = [];
			const program = buildProgram({
				generateRunner: async (options) => {
					generateCalls.push(options);
					return {
						batches: [],
						startTime: options.startTime as Date,
						endTime: options.endTime as Date,
						minInterval: 1000,
						maxInterval: 1000,
						totalBatches: 0,
						totalMessages: 0,
					};
				},
				log: () => undefined,
				onError: (err) => {
					throw err;
				},
				liveSignal: controller.signal,
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}

			await expect(
				program.parseAsync(
					[
						"node",
						"cli",
						"generate",
						"--startTime",
						"2024-01-01T00:00:00Z",
						"--endTime",
						"2026-01-01T00:00:00Z",
						"--live",
					],
					{ from: "node" },
				),
			).rejects.toThrow(/--endTime.*--live|--live.*--endTime/);
			// Runner must not have been invoked — we fail before entering
			// the tick loop.
			expect(generateCalls).toHaveLength(0);
		});

		it("rejects an invalid --live-interval at parse time", async () => {
			const controller = new AbortController();
			const program = buildProgram({
				onError: (err) => {
					throw err;
				},
				log: () => undefined,
				liveSignal: controller.signal,
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}

			// Invalid live-interval must fail fast via commander's parser,
			// BEFORE the action handler runs — no infinite loop, no silent
			// default fallback.
			await expect(
				program.parseAsync(
					[
						"node",
						"cli",
						"generate",
						"--live",
						"--live-interval",
						"not-a-duration",
					],
					{ from: "node" },
				),
			).rejects.toThrow(/--live-interval/);
		});

		it("does not invoke the runner when liveSignal is already aborted on entry", async () => {
			// Guards the "abort before parseAsync starts" path — e.g. a user
			// Ctrl-C that fires between `buildProgram` and `parseAsync`.
			// The CLI must observe the pre-aborted signal and exit the live
			// loop without firing a single tick, not hang or double-tick.
			const controller = new AbortController();
			controller.abort();
			const generateCalls: FakeTimeSeriesOptions[] = [];
			const logs: string[] = [];

			const program = buildProgram({
				generateRunner: async (options) => {
					generateCalls.push(options);
					return {
						batches: [],
						startTime: options.startTime as Date,
						endTime: options.endTime as Date,
						minInterval: 1000,
						maxInterval: 1000,
						totalBatches: 0,
						totalMessages: 0,
					};
				},
				log: (msg) => logs.push(msg),
				onError: (err) => {
					throw err;
				},
				liveSignal: controller.signal,
				now: () => new Date("2024-01-01T00:00:05Z"),
				sleep: async () => undefined,
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}

			await program.parseAsync(
				[
					"node",
					"cli",
					"generate",
					"--startTime",
					"2024-01-01T00:00:00Z",
					"--live",
				],
				{ from: "node" },
			);

			// Pre-aborted signal → zero ticks → zero logs.
			expect(generateCalls).toHaveLength(0);
			expect(logs).toHaveLength(0);
		});

		it("defaults --live-interval to 1s when the flag is omitted", async () => {
			// User omits --live-interval; the CLI should still enter live
			// mode and the sleep helper should be called with the default
			// 1000ms. Captured via the injected sleep stub.
			const sleepCalls: number[] = [];
			const controller = new AbortController();
			let clockMs = new Date("2024-01-01T00:00:00Z").getTime();
			let calls = 0;

			const program = buildProgram({
				generateRunner: async (options) => {
					calls++;
					clockMs += 1;
					if (calls >= 2) controller.abort();
					return {
						batches: [],
						startTime: options.startTime as Date,
						endTime: options.endTime as Date,
						minInterval: 1000,
						maxInterval: 1000,
						totalBatches: 0,
						totalMessages: 0,
					};
				},
				log: () => undefined,
				onError: (err) => {
					throw err;
				},
				liveSignal: controller.signal,
				now: () => new Date(clockMs),
				sleep: async (ms) => {
					sleepCalls.push(ms);
					clockMs += ms;
				},
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}

			await program.parseAsync(
				[
					"node",
					"cli",
					"generate",
					"--startTime",
					"2024-01-01T00:00:00Z",
					"--live",
				],
				{ from: "node" },
			);

			expect(calls).toBeGreaterThanOrEqual(1);
			// At least one sleep was the default 1000ms.
			expect(sleepCalls).toContain(1000);
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

		it("rejects --endTime + --live with a clear incompatibility message", async () => {
			// Same guard as the `generate --live` case: the send command
			// must also reject the combination so a user doesn't think
			// they're sending bounded data while the loop streams
			// indefinitely.
			const controller = new AbortController();
			const toSinkCalls: FakeTimeSeriesToSinkOptions[] = [];
			const program = buildProgram({
				toSinkRunner: async (options) => {
					toSinkCalls.push(options);
					return {
						batches: [],
						startTime: options.startTime as Date,
						endTime: options.endTime as Date,
						minInterval: 1000,
						maxInterval: 1000,
						totalBatches: 0,
						totalMessages: 0,
					};
				},
				log: () => undefined,
				onError: (err) => {
					throw err;
				},
				liveSignal: controller.signal,
			});
			program.exitOverride();
			for (const cmd of program.commands) {
				cmd.exitOverride();
			}

			await expect(
				program.parseAsync(
					[
						"node",
						"cli",
						"send",
						"--sink-url",
						"https://example.com",
						"--endTime",
						"2026-01-01T00:00:00Z",
						"--live",
					],
					{ from: "node" },
				),
			).rejects.toThrow(/--endTime.*--live|--live.*--endTime/);
			expect(toSinkCalls).toHaveLength(0);
		});

		it("loops in live mode, chaining windows, until the signal aborts", async () => {
			// Drive the send command with --live and verify:
			//   1. toSink is called once per tick
			//   2. Each tick's startTime == the previous tick's endTime
			//      (no gap, no overlap)
			//   3. The loop exits cleanly when the controller aborts
			//   4. sinkUrl/headers/live/liveInterval are not leaked into
			//      library options
			const toSinkCalls: FakeTimeSeriesToSinkOptions[] = [];
			const controller = new AbortController();
			let clockMs = new Date("2024-01-01T00:00:00Z").getTime();

			const program = buildProgram({
				toSinkRunner: async (options) => {
					toSinkCalls.push(options);
					// Pretend the tick took 1ms so the next window is non-empty.
					clockMs += 1;
					if (toSinkCalls.length >= 2) {
						controller.abort();
					}
					return {
						batches: [],
						startTime: options.startTime as Date,
						endTime: options.endTime as Date,
						minInterval: 1000,
						maxInterval: 1000,
						totalBatches: 0,
						totalMessages: 0,
					};
				},
				log: () => undefined,
				onError: (err) => {
					throw err;
				},
				liveSignal: controller.signal,
				now: () => new Date(clockMs),
				sleep: async (ms) => {
					clockMs += ms;
				},
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
					"https://example.com/ingest",
					"--startTime",
					"2024-01-01T00:00:00Z",
					"--live",
					"--live-interval",
					"50",
				],
				{ from: "node" },
			);

			expect(toSinkCalls).toHaveLength(2);
			for (const call of toSinkCalls) {
				expect(typeof call.fetcher).toBe("function");
				expect(call.startTime).toBeInstanceOf(Date);
				expect(call.endTime).toBeInstanceOf(Date);
				// CLI-only fields must not leak into library options.
				const raw = call as unknown as Record<string, unknown>;
				expect(raw.sinkUrl).toBeUndefined();
				expect(raw.headers).toBeUndefined();
				expect(raw.live).toBeUndefined();
				expect(raw.liveInterval).toBeUndefined();
			}
			// Chained windows: every tick picks up exactly where the
			// previous one left off.
			for (let i = 1; i < toSinkCalls.length; i++) {
				expect((toSinkCalls[i].startTime as Date).getTime()).toBe(
					(toSinkCalls[i - 1].endTime as Date).getTime(),
				);
			}
			// First tick starts at the user-provided startTime (resolved
			// once, before the loop).
			expect((toSinkCalls[0].startTime as Date).toISOString()).toBe(
				"2024-01-01T00:00:00.000Z",
			);
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
