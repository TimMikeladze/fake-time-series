import { parseDate } from "chrono-node";
import ms from "ms";

export type FakeTimeSeriesData = {
	timestamp: number;
	key: string;
	data: Record<string, unknown>;
};

export type ShapeFunctions = {
	[key: string]: (currentTime: Date) => Record<string, unknown>;
};

type TimeInput = Date | number | string;

const parseTime = (time: TimeInput): Date => {
	if (time instanceof Date) {
		return time;
	}
	if (typeof time === "number") {
		return new Date(time);
	}
	if (typeof time === "string") {
		const parsed = parseDate(time);
		if (parsed) {
			return parsed;
		}
		throw new Error(`Unable to parse time: ${time}`);
	}
	return new Date(time);
};

const parseInterval = (interval: TimeInput): number => {
	if (typeof interval === "string") {
		return ms(interval);
	}
	if (typeof interval === "number") {
		return interval;
	}
	if (interval instanceof Date) {
		return interval.getTime();
	}
	return interval;
};

export interface FakeTimeSeriesOptions {
	startTime?: Date | number | string;
	endTime?: Date | number | string;
	minInterval?: string | number;
	maxInterval?: string | number;
	maxBatchSize?: number;
	batchSizeRandomization?: boolean;
	intervalRandomization?: boolean;
	batchReverseProbability?: number;
	batchShuffleProbability?: number;
	intervalSkewProbability?: number;
	shapes?: ShapeFunctions;
}

export const defaultOptions: FakeTimeSeriesOptions = {
	startTime: "-1 day",
	endTime: new Date(),
	minInterval: "1s",
	maxInterval: "10s",
	maxBatchSize: 10,
	batchSizeRandomization: true,
	intervalRandomization: true,
	batchReverseProbability: 0.5,
	batchShuffleProbability: 0.4,
	intervalSkewProbability: 0.8,
	shapes: {
		default: () => ({
			value: Math.random(),
		}),
	},
};

export interface FakeTimeSeriesResult {
	batches: FakeTimeSeriesData[][];
	startTime: Date;
	endTime: Date;
	minInterval: number;
	maxInterval: number;
	totalBatches: number;
	totalMessages: number;
}

export async function* generateBatches(
	options: FakeTimeSeriesOptions = {},
): AsyncGenerator<FakeTimeSeriesData[], void, unknown> {
	const mergedOptions: Required<FakeTimeSeriesOptions> = {
		...defaultOptions,
		...options,
	} as Required<FakeTimeSeriesOptions>;

	const start = parseTime(mergedOptions.startTime);
	const end = parseTime(mergedOptions.endTime);

	// Validate time range
	if (start >= end) {
		throw new Error("Start time must be before end time");
	}

	const minInt = parseInterval(mergedOptions.minInterval);
	const maxInt = parseInterval(mergedOptions.maxInterval);

	// Validate intervals
	if (minInt <= 0 || maxInt <= 0) {
		throw new Error("Intervals must be positive");
	}
	if (minInt > maxInt) {
		throw new Error(
			"Minimum interval must be less than or equal to maximum interval",
		);
	}

	const shapeKeys = Object.keys(mergedOptions.shapes);
	let currentTime = new Date(start.getTime());

	while (currentTime < end) {
		const batchSize = mergedOptions.batchSizeRandomization
			? Math.floor(Math.random() * mergedOptions.maxBatchSize) + 1
			: mergedOptions.maxBatchSize;
		const batch: FakeTimeSeriesData[] = [];

		for (let i = 0; i < batchSize; i++) {
			// Calculate the maximum allowed interval to stay within end time
			const timeRemaining = end.getTime() - currentTime.getTime();
			const effectiveMaxInt = Math.min(maxInt, timeRemaining);

			if (timeRemaining < minInt) {
				break; // Stop if we can't fit another interval
			}

			// Adjust interval calculation to respect bounds
			const interval = mergedOptions.intervalRandomization
				? Math.random() < (mergedOptions.intervalSkewProbability ?? 0.8)
					? Math.min(
							minInt + Math.floor(Math.random() * (effectiveMaxInt - minInt)),
							timeRemaining,
						)
					: Math.min(maxInt, timeRemaining)
				: minInt;

			// Ensure interval is at least minInt to prevent infinite loops
			if (interval < minInt) {
				break;
			}

			const nextTime = new Date(currentTime.getTime() + interval);

			// Double-check we're still in bounds and making progress
			if (nextTime > end || nextTime <= currentTime) {
				break;
			}

			currentTime = nextTime;

			// Pick a random data shape for this data point
			const shapeKey = shapeKeys[Math.floor(Math.random() * shapeKeys.length)];
			const dataPoint: FakeTimeSeriesData = {
				timestamp: currentTime.getTime(),
				key: shapeKey,
				data: mergedOptions.shapes[shapeKey](currentTime),
			};

			batch.push(dataPoint);
		}

		if (batch.length > 0) {
			if (Math.random() < mergedOptions.batchReverseProbability) {
				batch.reverse();
			}
			if (Math.random() < mergedOptions.batchShuffleProbability) {
				batch.sort(() => Math.random() - 0.5);
			}

			yield batch;
		} else {
			// If we couldn't add any points to the batch, we're stuck
			break;
		}
	}
}

export async function generate(
	options: FakeTimeSeriesOptions = {},
): Promise<FakeTimeSeriesResult> {
	const mergedOptions = {
		...defaultOptions,
		...options,
	} as Required<FakeTimeSeriesOptions>;

	const start = parseTime(mergedOptions.startTime);
	const end = parseTime(mergedOptions.endTime);
	const minInt = parseInterval(mergedOptions.minInterval);
	const maxInt = parseInterval(mergedOptions.maxInterval);

	const allBatches: FakeTimeSeriesData[][] = [];
	let totalMessages = 0;

	// Consume the generator synchronously
	const generator = generateBatches(options);
	let result = await generator.next();
	while (!result.done) {
		const batch = result.value;
		allBatches.push(batch);
		totalMessages += batch.length;
		result = await generator.next();
	}

	return {
		batches: allBatches,
		startTime: start,
		endTime: end,
		minInterval: minInt,
		maxInterval: maxInt,
		totalBatches: allBatches.length,
		totalMessages,
	};
}

export interface FakeTimeSeriesToSinkOptions extends FakeTimeSeriesOptions {
	fetcher: (batch: FakeTimeSeriesData[]) => Promise<Response>;
	concurrency?: number;
	onError?: (error: unknown) => void;
}

export const defaultFakeTimeSeriesToSinkOptions: Omit<
	FakeTimeSeriesToSinkOptions,
	"fetcher"
> = {
	...defaultOptions,
	concurrency: 10,
	onError: (error) => {
		console.error(JSON.stringify(error, null, 2));
	},
};

export async function toSink(
	options: FakeTimeSeriesToSinkOptions,
): Promise<FakeTimeSeriesResult> {
	const pLimit = (await import("p-limit")).default;
	const mergedOptions = {
		...defaultFakeTimeSeriesToSinkOptions,
		...options,
	} as Required<FakeTimeSeriesToSinkOptions>;
	const limit = pLimit(mergedOptions.concurrency || 1);
	const requests = [];

	const { batches, ...other } = await generate(mergedOptions);
	for (const batch of batches) {
		requests.push(
			limit(async () => {
				const response = await mergedOptions.fetcher(batch);

				if (!response.ok) {
					mergedOptions.onError?.(await response.text());
				}
			}),
		);
	}

	await Promise.all(requests);

	return {
		batches,
		...other,
	};
}
