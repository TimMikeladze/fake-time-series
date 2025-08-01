import { describe, expect, it } from "vitest";
import { type FakeTimeSeriesData, generate, toSink } from "../src";

describe("generateTimeSeries", () => {
	it("should generate time series data with default options", async () => {
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

	it("should respect time bounds", async () => {
		const result = await generate({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:30Z",
			minInterval: "10s",
			maxInterval: "10s",
		});

		expect(result.batches.length).toBeGreaterThan(0);
		expect(result.totalMessages).toBeGreaterThan(0);

		// Check that all timestamps are within bounds
		for (const batch of result.batches) {
			for (const dataPoint of batch) {
				expect(dataPoint.timestamp).toBeGreaterThanOrEqual(
					new Date("2024-01-01T00:00:00Z").getTime(),
				);
				expect(dataPoint.timestamp).toBeLessThanOrEqual(
					new Date("2024-01-01T00:00:30Z").getTime(),
				);
			}
		}
	});

	it("should throw error if start time is after end time", async () => {
		await expect(
			generate({
				startTime: "2024-01-01T00:01:00Z",
				endTime: "2024-01-01T00:00:00Z",
			}),
		).rejects.toThrow("Start time must be before end time");
	});

	it("should apply transform function when provided", async () => {
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

		// Check that transform was applied
		for (const batch of result.batches) {
			for (const dataPoint of batch) {
				expect(dataPoint).toHaveProperty("partitionKey", "test");
				expect(dataPoint).toHaveProperty("timestamp");
				expect(dataPoint).toHaveProperty("data.value");
				// Should not have the original structure
				expect(dataPoint).not.toHaveProperty("key");
			}
		}
	});
});

describe("timeSeriesToSink", () => {
	it("should send data to sink with default concurrency", async () => {
		const mockFetcher = async (batch: FakeTimeSeriesData[]) => {
			return new Response(JSON.stringify(batch), { status: 200 });
		};

		const result = await toSink({
			startTime: "2024-01-01",
			endTime: "2024-01-02",
			fetcher: mockFetcher,
		});

		expect(result).toHaveProperty("totalBatches");
		expect(result).toHaveProperty("totalMessages");
		expect(result.totalBatches).toBeGreaterThan(0);
		expect(result.totalMessages).toBeGreaterThan(0);
	});

	it("should handle sink errors", async () => {
		const errors: unknown[] = [];
		const mockFetcher = async () => {
			return new Response("Error", { status: 500 });
		};

		await toSink({
			startTime: "2024-01-01",
			endTime: "2024-01-02",
			fetcher: mockFetcher,
			onError: (error) => {
				errors.push(error);
			},
		});

		expect(errors.length).toBeGreaterThan(0);
	});
});
