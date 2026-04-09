import { describe, expect, it, vi } from "vitest";
import {
	createDefaultFakeTimeSeriesToSinkOptions,
	defaultFakeTimeSeriesToSinkOptions,
	defaultOptions,
	type FakeTimeSeriesData,
	generate,
	generateBatches,
	toSink,
} from "../src";

describe("generate", () => {
	it("generates time series data with default options", async () => {
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:01:00Z",
			minInterval: "10s",
			maxInterval: "10s",
		});

		expect(result.batches.length).toBeGreaterThan(0);
		expect(result.totalMessages).toBeGreaterThan(0);
		expect(result.startTime).toEqual(new Date("2024-01-01T00:00:00Z"));
		expect(result.endTime).toEqual(new Date("2024-01-01T00:01:00Z"));
	});

	it("reports totalBatches and totalMessages consistently", async () => {
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:02:00Z",
			minInterval: "5s",
			maxInterval: "5s",
			batchSizeRandomization: false,
			maxBatchSize: 4,
		});

		expect(result.totalBatches).toBe(result.batches.length);
		const counted = result.batches.reduce((sum, b) => sum + b.length, 0);
		expect(result.totalMessages).toBe(counted);
	});

	it("respects time bounds", async () => {
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:30Z",
			minInterval: "10s",
			maxInterval: "10s",
		});

		expect(result.batches.length).toBeGreaterThan(0);
		expect(result.totalMessages).toBeGreaterThan(0);

		const startMs = new Date("2024-01-01T00:00:00Z").getTime();
		const endMs = new Date("2024-01-01T00:00:30Z").getTime();
		for (const batch of result.batches) {
			for (const dataPoint of batch) {
				expect(dataPoint.timestamp).toBeGreaterThanOrEqual(startMs);
				expect(dataPoint.timestamp).toBeLessThanOrEqual(endMs);
			}
		}
	});

	it("accepts Date objects for start/end time", async () => {
		const start = new Date("2024-01-01T00:00:00Z");
		const end = new Date("2024-01-01T00:00:20Z");
		const result = await generate({
			startTime: start,
			endTime: end,
			minInterval: "5s",
			maxInterval: "5s",
		});

		expect(result.startTime).toEqual(start);
		expect(result.endTime).toEqual(end);
	});

	it("accepts numeric (epoch ms) start/end time", async () => {
		const start = new Date("2024-01-01T00:00:00Z").getTime();
		const end = new Date("2024-01-01T00:00:20Z").getTime();
		const result = await generate({
			startTime: start,
			endTime: end,
			minInterval: "5s",
			maxInterval: "5s",
		});

		expect(result.startTime.getTime()).toBe(start);
		expect(result.endTime.getTime()).toBe(end);
	});

	it("accepts numeric interval (ms) inputs", async () => {
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:10Z",
			minInterval: 1000,
			maxInterval: 1000,
		});
		expect(result.minInterval).toBe(1000);
		expect(result.maxInterval).toBe(1000);
		expect(result.totalMessages).toBeGreaterThan(0);
	});

	it("throws if start time is after end time", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:01:00Z",
				endTime: "2024-01-01T00:00:00Z",
			}),
		).rejects.toThrow("Start time must be before end time");
	});

	it("throws if start time equals end time", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:00Z",
			}),
		).rejects.toThrow("Start time must be before end time");
	});

	it("throws on unparseable start time string", async () => {
		await expect(
			generate({
				startTime: "not a date at all",
				endTime: "2024-01-01T00:00:00Z",
			}),
		).rejects.toThrow(/Unable to parse time/);
	});

	it("throws on invalid ms interval string", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:01:00Z",
				minInterval: "banana",
			}),
		).rejects.toThrow(/Invalid interval/);
	});

	it("throws on negative interval", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:01:00Z",
				minInterval: -1,
				maxInterval: 1000,
			}),
		).rejects.toThrow("Intervals must be positive");
	});

	it("throws when minInterval exceeds maxInterval", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:01:00Z",
				minInterval: "10s",
				maxInterval: "5s",
			}),
		).rejects.toThrow(
			"Minimum interval must be less than or equal to maximum interval",
		);
	});

	it("throws on empty shapes object", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:10Z",
				minInterval: "1s",
				maxInterval: "1s",
				shapes: {},
			}),
		).rejects.toThrow("At least one shape must be provided");
	});

	it("throws on non-positive maxBatchSize", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:10Z",
				minInterval: "1s",
				maxInterval: "1s",
				maxBatchSize: 0,
			}),
		).rejects.toThrow(/maxBatchSize must be a positive integer/);
	});

	it("throws on out-of-range probability", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:10Z",
				minInterval: "1s",
				maxInterval: "1s",
				batchReverseProbability: 1.5,
			}),
		).rejects.toThrow(/batchReverseProbability/);
	});

	it("selects from multiple shapes", async () => {
		const shapes = {
			alpha: () => ({ kind: "alpha" }),
			beta: () => ({ kind: "beta" }),
		};
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:01:00Z",
			minInterval: "1s",
			maxInterval: "1s",
			batchSizeRandomization: false,
			maxBatchSize: 10,
			shapes,
		});

		const seenKeys = new Set<string>();
		for (const batch of result.batches) {
			for (const dp of batch) {
				seenKeys.add(dp.key);
				expect(["alpha", "beta"]).toContain(dp.key);
			}
		}
		// With enough samples it is vanishingly unlikely both shapes are
		// unused; this is a smoke test for shape selection.
		expect(seenKeys.size).toBeGreaterThan(0);
	});

	it("passes currentTime to shape functions", async () => {
		const seen: Date[] = [];
		await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:10Z",
			minInterval: "2s",
			maxInterval: "2s",
			batchSizeRandomization: false,
			maxBatchSize: 1,
			shapes: {
				probe: (t: Date) => {
					seen.push(t);
					return { t: t.getTime() };
				},
			},
		});
		expect(seen.length).toBeGreaterThan(0);
		for (const t of seen) {
			expect(t).toBeInstanceOf(Date);
		}
	});

	it("applies transform function when provided", async () => {
		const transform = (dataPoint: FakeTimeSeriesData) => ({
			partitionKey: "test",
			timestamp: dataPoint.timestamp,
			data: {
				value: dataPoint.data.value || 0,
			},
		});

		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:10Z",
			minInterval: "5s",
			maxInterval: "5s",
			transform,
		});

		expect(result.batches.length).toBeGreaterThan(0);
		expect(result.totalMessages).toBeGreaterThan(0);

		for (const batch of result.batches) {
			for (const dataPoint of batch) {
				expect(dataPoint).toHaveProperty("partitionKey", "test");
				expect(dataPoint).toHaveProperty("timestamp");
				expect(dataPoint).toHaveProperty("data.value");
				expect(dataPoint).not.toHaveProperty("key");
			}
		}
	});

	it("resolves default endTime lazily, not at module load", async () => {
		// Reaching into defaults directly: the exported defaults intentionally
		// do NOT freeze `endTime`, so resolution must happen at call time.
		expect(defaultOptions.endTime).toBeUndefined();

		const before = Date.now();
		// Sleep a tiny bit so that any frozen default would lag behind "now".
		await new Promise((resolve) => setTimeout(resolve, 10));
		const result = await generate({
			startTime: "-5 minutes",
			minInterval: "30s",
			maxInterval: "30s",
		});
		const after = Date.now();

		// endTime should be "now" at call time — within a reasonable window
		// of when we invoked generate().
		expect(result.endTime.getTime()).toBeGreaterThanOrEqual(before);
		expect(result.endTime.getTime()).toBeLessThanOrEqual(after + 1_000);
	});

	it("re-evaluates relative endTime strings on every call", async () => {
		// A user passing endTime: "in 1 hour" should get a fresh resolution
		// each time, not a single frozen value from the first call.
		const first = await generate({
			startTime: "-10 minutes",
			endTime: "in 1 hour",
			minInterval: "5m",
			maxInterval: "5m",
		});
		await new Promise((resolve) => setTimeout(resolve, 15));
		const second = await generate({
			startTime: "-10 minutes",
			endTime: "in 1 hour",
			minInterval: "5m",
			maxInterval: "5m",
		});

		// Both calls resolved "in 1 hour" relative to their own "now"; the
		// second call must be strictly later than the first.
		expect(second.endTime.getTime()).toBeGreaterThan(first.endTime.getTime());
	});

	it("reaches maxInterval when min and max are equal", async () => {
		// Previously off-by-one would have made the maximum unreachable;
		// with fixed inclusive range, fixed-interval mode yields exactly
		// the requested spacing.
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:10Z",
			minInterval: "2s",
			maxInterval: "2s",
			batchSizeRandomization: false,
			intervalRandomization: false,
			maxBatchSize: 10,
		});
		expect(result.totalMessages).toBeGreaterThan(0);
		const allTimestamps: number[] = [];
		for (const batch of result.batches) {
			for (const dp of batch) {
				allTimestamps.push(dp.timestamp);
			}
		}
		allTimestamps.sort((a, b) => a - b);
		for (let i = 1; i < allTimestamps.length; i++) {
			expect(allTimestamps[i] - allTimestamps[i - 1]).toBe(2_000);
		}
	});
});

describe("generateBatches (streaming)", () => {
	it("yields batches incrementally via for await", async () => {
		const batches: FakeTimeSeriesData[][] = [];
		for await (const batch of generateBatches({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:20Z",
			minInterval: "5s",
			maxInterval: "5s",
		})) {
			batches.push(batch);
		}
		expect(batches.length).toBeGreaterThan(0);
		for (const batch of batches) {
			expect(batch.length).toBeGreaterThan(0);
		}
	});

	it("validates options before yielding", async () => {
		const gen = generateBatches({
			startTime: "2024-01-02T00:00:00Z",
			endTime: "2024-01-01T00:00:00Z",
		});
		await expect(gen.next()).rejects.toThrow(
			"Start time must be before end time",
		);
	});
});

describe("toSink", () => {
	it("sends data to sink with default concurrency", async () => {
		const calls: number[] = [];
		const mockFetcher = async (batch: FakeTimeSeriesData[]) => {
			calls.push(batch.length);
			return new Response(JSON.stringify(batch), { status: 200 });
		};

		const result = await toSink({
			startTime: "2024-01-01",
			endTime: "2024-01-02",
			fetcher: mockFetcher,
		});

		expect(result.totalBatches).toBeGreaterThan(0);
		expect(result.totalMessages).toBeGreaterThan(0);
		expect(calls.length).toBe(result.totalBatches);
	});

	it("routes non-2xx responses to onError", async () => {
		const errors: unknown[] = [];
		const mockFetcher = async () => new Response("boom", { status: 500 });

		const result = await toSink({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:30Z",
			minInterval: "10s",
			maxInterval: "10s",
			fetcher: mockFetcher,
			onError: (error) => {
				errors.push(error);
			},
		});

		expect(errors.length).toBeGreaterThan(0);
		// Each failure should now arrive as an Error instance with status info.
		for (const err of errors) {
			expect(err).toBeInstanceOf(Error);
			expect((err as Error).message).toContain("500");
		}
		// totalBatches is still reported even when batches failed.
		expect(result.totalBatches).toBeGreaterThan(0);
	});

	it("routes fetcher exceptions to onError without aborting other batches", async () => {
		const errors: unknown[] = [];
		let callCount = 0;

		const mockFetcher = async () => {
			callCount++;
			// Fail the second call outright — the rest should still succeed.
			if (callCount === 2) {
				throw new Error("network down");
			}
			return new Response("ok", { status: 200 });
		};

		const result = await toSink({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:01:00Z",
			minInterval: "10s",
			maxInterval: "10s",
			// Force one point per batch so we get multiple fetcher invocations.
			batchSizeRandomization: false,
			maxBatchSize: 1,
			fetcher: mockFetcher,
			concurrency: 1,
			onError: (error) => {
				errors.push(error);
			},
		});

		expect(callCount).toBeGreaterThanOrEqual(2);
		expect(errors.length).toBe(1);
		expect((errors[0] as Error).message).toBe("network down");
		// The run completed and returned a result rather than rejecting.
		expect(result.totalBatches).toBeGreaterThan(1);
	});

	it("throws when fetcher is missing", async () => {
		await expect(
			// @ts-expect-error - intentionally passing invalid options
			toSink({ startTime: "2024-01-01", endTime: "2024-01-02" }),
		).rejects.toThrow("fetcher is required");
	});

	it("throws on zero concurrency", async () => {
		await expect(
			toSink({
				startTime: "2024-01-01",
				endTime: "2024-01-02",
				fetcher: async () => new Response("", { status: 200 }),
				concurrency: 0,
			}),
		).rejects.toThrow(/concurrency must be a positive number/);
	});

	it("throws on negative concurrency", async () => {
		await expect(
			toSink({
				startTime: "2024-01-01",
				endTime: "2024-01-02",
				fetcher: async () => new Response("", { status: 200 }),
				concurrency: -5,
			}),
		).rejects.toThrow(/concurrency must be a positive number/);
	});

	it("respects concurrency limit", async () => {
		let active = 0;
		let maxActive = 0;
		const mockFetcher = async () => {
			active++;
			maxActive = Math.max(maxActive, active);
			await new Promise((resolve) => setTimeout(resolve, 5));
			active--;
			return new Response("", { status: 200 });
		};

		await toSink({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:30Z",
			minInterval: "5s",
			maxInterval: "5s",
			fetcher: mockFetcher,
			concurrency: 2,
		});

		expect(maxActive).toBeLessThanOrEqual(2);
	});

	it("swallows errors thrown by onError handler itself", async () => {
		// Wrap the spy in try/finally so a test failure can't leave
		// `console.error` silenced for subsequent tests.
		const consoleSpy = vi
			.spyOn(console, "error")
			.mockImplementation(() => undefined);
		try {
			const mockFetcher = async () => new Response("boom", { status: 500 });

			// A handler that itself throws must not crash toSink.
			const result = await toSink({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:20Z",
				minInterval: "10s",
				maxInterval: "10s",
				fetcher: mockFetcher,
				onError: () => {
					throw new Error("handler bug");
				},
			});

			expect(result.totalBatches).toBeGreaterThan(0);
		} finally {
			consoleSpy.mockRestore();
		}
	});

	it("exposes backwards-compatible default sink options", () => {
		expect(defaultFakeTimeSeriesToSinkOptions.concurrency).toBe(10);
		expect(typeof defaultFakeTimeSeriesToSinkOptions.onError).toBe("function");
		// `endTime` is intentionally undefined in the const; consumers who
		// need a concrete value should call the factory below.
		expect(defaultFakeTimeSeriesToSinkOptions.endTime).toBeUndefined();
	});

	it("createDefaultFakeTimeSeriesToSinkOptions returns fresh endTime on each call", async () => {
		const first = createDefaultFakeTimeSeriesToSinkOptions();
		await new Promise((resolve) => setTimeout(resolve, 10));
		const second = createDefaultFakeTimeSeriesToSinkOptions();
		expect(first.endTime).toBeInstanceOf(Date);
		expect(second.endTime).toBeInstanceOf(Date);
		expect((second.endTime as Date).getTime()).toBeGreaterThan(
			(first.endTime as Date).getTime(),
		);
	});

	it("retainBatches:false suppresses batch accumulation but counts stay accurate", async () => {
		let sinkCalls = 0;
		const mockFetcher = async () => {
			sinkCalls++;
			return new Response("", { status: 200 });
		};

		const result = await toSink({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:01:00Z",
			minInterval: "10s",
			maxInterval: "10s",
			batchSizeRandomization: false,
			maxBatchSize: 1,
			fetcher: mockFetcher,
			concurrency: 1,
			retainBatches: false,
		});

		expect(result.batches).toEqual([]);
		expect(result.totalBatches).toBeGreaterThan(0);
		expect(result.totalMessages).toBeGreaterThan(0);
		// sinkCalls matches totalBatches even though batches wasn't retained.
		expect(sinkCalls).toBe(result.totalBatches);
	});

	it("retainBatches defaults to true", async () => {
		const mockFetcher = async () => new Response("", { status: 200 });
		const result = await toSink({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:20Z",
			minInterval: "10s",
			maxInterval: "10s",
			fetcher: mockFetcher,
		});
		expect(result.batches.length).toBeGreaterThan(0);
	});
});

describe("transform generic inference", () => {
	it("infers the output type from the transform return type", async () => {
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:10Z",
			minInterval: "5s",
			maxInterval: "5s",
			transform: (dp) => ({
				kind: "metric" as const,
				ts: dp.timestamp,
			}),
		});

		// With the generic API, `result.batches` is inferred as
		// `{kind: "metric", ts: number}[][]`. This is a compile-time check
		// (assigning the inferred property confirms the type flows through);
		// at runtime we additionally verify the shape is correct.
		for (const batch of result.batches) {
			for (const dp of batch) {
				expect(dp.kind).toBe("metric");
				expect(typeof dp.ts).toBe("number");
				// The transformed shape no longer has the original keys.
				expect((dp as unknown as { key?: string }).key).toBeUndefined();
			}
		}
	});
});
