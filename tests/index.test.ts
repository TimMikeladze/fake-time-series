import { describe, expect, it } from "vitest";
import { type FakeTimeSeriesData, generate, toSink } from "../src";

describe("generateTimeSeries", () => {
	it("should generate time series data with default options", async () => {
		const { batches } = await generate();
		const firstBatch = batches[0];

		expect(Array.isArray(firstBatch)).toBe(true);
		expect(firstBatch.length).toBeGreaterThan(0);
		expect(firstBatch.length).toBeLessThanOrEqual(10);

		const firstDataPoint = firstBatch[0];
		expect(firstDataPoint).toHaveProperty("timestamp");
		expect(firstDataPoint.timestamp).toBeGreaterThan(0);
		expect(firstDataPoint).toHaveProperty("key");
		expect(firstDataPoint).toHaveProperty("data");
	});

	it("should respect time bounds", async () => {
		const startTime = new Date("2024-01-01");
		const endTime = new Date("2024-01-02");

		const { batches } = await generate({ startTime, endTime });

		for (const batch of batches) {
			for (const point of batch) {
				expect(point.timestamp).toBeGreaterThanOrEqual(startTime.getTime());
				expect(point.timestamp).toBeLessThanOrEqual(endTime.getTime());
			}
		}
	});

	it("should throw error if start time is after end time", async () => {
		await expect(
			generate({
				startTime: "2024-01-02",
				endTime: "2024-01-01",
			}),
		).rejects.toThrow("Start time must be before end time");
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
