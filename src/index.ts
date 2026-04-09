import { parseDate } from "chrono-node";
import ms from "ms";

export type FakeTimeSeriesData = {
	timestamp: number;
	key: string;
	data: Record<string, unknown>;
};

export type TransformFunction<TOutput = unknown> = (
	dataPoint: FakeTimeSeriesData,
) => TOutput;

export type ShapeFunctions = {
	[key: string]: (currentTime: Date) => Record<string, unknown>;
};

type TimeInput = Date | number | string;
type IntervalInput = string | number;

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
	throw new Error(`Unable to parse time: ${String(time)}`);
};

// ms()'s typed overload accepts a narrow template-literal `StringValue`,
// but at runtime it accepts any string and returns `undefined` for invalid
// input. We validate that undefined return below, so a loose cast here is
// the honest, type-safe-at-the-boundary choice.
const msParse = ms as unknown as (value: string) => number | undefined;

const parseInterval = (interval: IntervalInput): number => {
	if (typeof interval === "number") {
		if (!Number.isFinite(interval)) {
			throw new Error(`Invalid interval: ${interval}`);
		}
		return interval;
	}
	const parsed = msParse(interval);
	// Defence in depth: ms() is documented to return `undefined` for
	// invalid input, but also reject NaN/Infinity in case a future
	// version or a pathological string slips through.
	if (parsed === undefined || !Number.isFinite(parsed)) {
		throw new Error(`Invalid interval: ${interval}`);
	}
	return parsed;
};

// Unbiased Fisher–Yates shuffle, in place.
const shuffleInPlace = <T>(array: T[]): void => {
	for (let i = array.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		const tmp = array[i];
		array[i] = array[j];
		array[j] = tmp;
	}
};

export interface FakeTimeSeriesOptions<TOutput = FakeTimeSeriesData> {
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
	transform?: TransformFunction<TOutput>;
}

// Frozen so an external consumer can't mutate the shared default map
// (e.g. `defaultOptions.shapes.foo = ...`) and pollute every subsequent
// `generate()` call that uses the default.
const DEFAULT_SHAPES: ShapeFunctions = Object.freeze({
	default: () => ({
		value: Math.random(),
	}),
}) as ShapeFunctions;

/**
 * Defaults applied to every call. `endTime` is intentionally NOT included here —
 * it is resolved lazily at call time (to `new Date()`) so that long-lived
 * processes and re-used option objects do not keep a stale snapshot.
 *
 * Frozen so external consumers can't accidentally mutate the shared
 * default and affect every subsequent call.
 */
export const defaultOptions: Readonly<FakeTimeSeriesOptions> = Object.freeze({
	startTime: "-1 day",
	minInterval: "1s",
	maxInterval: "10s",
	maxBatchSize: 10,
	batchSizeRandomization: true,
	intervalRandomization: true,
	batchReverseProbability: 0.5,
	batchShuffleProbability: 0.4,
	intervalSkewProbability: 0.8,
	shapes: DEFAULT_SHAPES,
});

export interface FakeTimeSeriesResult<TOutput = FakeTimeSeriesData> {
	batches: TOutput[][];
	startTime: Date;
	endTime: Date;
	minInterval: number;
	maxInterval: number;
	totalBatches: number;
	totalMessages: number;
}

interface ResolvedOptions {
	startDate: Date;
	endDate: Date;
	minInt: number;
	maxInt: number;
	maxBatchSize: number;
	batchSizeRandomization: boolean;
	intervalRandomization: boolean;
	batchReverseProbability: number;
	batchShuffleProbability: number;
	intervalSkewProbability: number;
	shapes: ShapeFunctions;
	shapeKeys: string[];
	transform?: TransformFunction;
}

// Constrain K to non-transform keys so the return type doesn't depend on
// the generic TOutput parameter — this lets `resolveOptions` accept
// `FakeTimeSeriesOptions<unknown>` without tripping variance checks on
// `transform`.
type PickableKey = keyof Omit<FakeTimeSeriesOptions, "transform">;

const pickOption = <K extends PickableKey>(
	options: FakeTimeSeriesOptions<unknown>,
	key: K,
): NonNullable<FakeTimeSeriesOptions[K]> => {
	const value = options[key] ?? defaultOptions[key];
	if (value === undefined) {
		throw new Error(`Missing required option: ${String(key)}`);
	}
	return value as NonNullable<FakeTimeSeriesOptions[K]>;
};

const assertProbability = (value: number, name: string): number => {
	if (!Number.isFinite(value) || value < 0 || value > 1) {
		throw new Error(`${name} must be a number between 0 and 1 (got ${value})`);
	}
	return value;
};

const resolveOptions = (
	options: FakeTimeSeriesOptions<unknown>,
): ResolvedOptions => {
	const startDate = parseTime(pickOption(options, "startTime"));
	// endTime is lazy: default to "now" at call time, not module load time.
	const endDate = parseTime(options.endTime ?? new Date());

	if (startDate >= endDate) {
		throw new Error("Start time must be before end time");
	}

	const minInt = parseInterval(pickOption(options, "minInterval"));
	const maxInt = parseInterval(pickOption(options, "maxInterval"));

	if (minInt <= 0 || maxInt <= 0) {
		throw new Error("Intervals must be positive");
	}
	if (minInt > maxInt) {
		throw new Error(
			"Minimum interval must be less than or equal to maximum interval",
		);
	}

	const maxBatchSize = pickOption(options, "maxBatchSize");
	if (!Number.isFinite(maxBatchSize) || maxBatchSize <= 0) {
		throw new Error(
			`maxBatchSize must be a positive integer (got ${maxBatchSize})`,
		);
	}

	const shapes = pickOption(options, "shapes");
	const shapeKeys = Object.keys(shapes);
	if (shapeKeys.length === 0) {
		throw new Error("At least one shape must be provided");
	}

	return {
		startDate,
		endDate,
		minInt,
		maxInt,
		maxBatchSize,
		batchSizeRandomization: pickOption(options, "batchSizeRandomization"),
		intervalRandomization: pickOption(options, "intervalRandomization"),
		batchReverseProbability: assertProbability(
			pickOption(options, "batchReverseProbability"),
			"batchReverseProbability",
		),
		batchShuffleProbability: assertProbability(
			pickOption(options, "batchShuffleProbability"),
			"batchShuffleProbability",
		),
		intervalSkewProbability: assertProbability(
			pickOption(options, "intervalSkewProbability"),
			"intervalSkewProbability",
		),
		shapes,
		shapeKeys,
		// Widen any TransformFunction<TOutput> to TransformFunction<unknown>
		// for the internal generator, then cast back at the public boundary.
		transform: options.transform as TransformFunction | undefined,
	};
};

async function* generateFromResolved(
	resolved: ResolvedOptions,
): AsyncGenerator<unknown[], void, unknown> {
	const {
		startDate,
		endDate,
		minInt,
		maxInt,
		maxBatchSize,
		batchSizeRandomization,
		intervalRandomization,
		intervalSkewProbability,
		batchReverseProbability,
		batchShuffleProbability,
		shapes,
		shapeKeys,
		transform,
	} = resolved;

	let currentTime = new Date(startDate);

	while (currentTime < endDate) {
		const batchSize = batchSizeRandomization
			? Math.floor(Math.random() * maxBatchSize) + 1
			: maxBatchSize;
		const batch: unknown[] = [];

		for (let i = 0; i < batchSize; i++) {
			const timeRemaining = endDate.getTime() - currentTime.getTime();
			if (timeRemaining < minInt) {
				break;
			}

			const effectiveMaxInt = Math.min(maxInt, timeRemaining);

			let interval: number;
			if (intervalRandomization) {
				if (Math.random() < intervalSkewProbability) {
					// Inclusive range [minInt, effectiveMaxInt].
					const span = effectiveMaxInt - minInt + 1;
					interval = minInt + Math.floor(Math.random() * span);
				} else {
					interval = effectiveMaxInt;
				}
			} else {
				interval = minInt;
			}

			if (interval < minInt) {
				break;
			}

			const nextTime = new Date(currentTime.getTime() + interval);
			if (nextTime > endDate || nextTime <= currentTime) {
				break;
			}

			currentTime = nextTime;

			const shapeKey = shapeKeys[Math.floor(Math.random() * shapeKeys.length)];
			const dataPoint: FakeTimeSeriesData = {
				timestamp: currentTime.getTime(),
				key: shapeKey,
				data: shapes[shapeKey](currentTime),
			};

			const finalDataPoint = transform ? transform(dataPoint) : dataPoint;
			batch.push(finalDataPoint);
		}

		if (batch.length === 0) {
			// Couldn't fit anything else into the remaining window.
			break;
		}

		if (Math.random() < batchReverseProbability) {
			batch.reverse();
		}
		if (Math.random() < batchShuffleProbability) {
			shuffleInPlace(batch);
		}

		yield batch;
	}
}

export async function* generateBatches<TOutput = FakeTimeSeriesData>(
	options: FakeTimeSeriesOptions<TOutput> = {},
): AsyncGenerator<TOutput[], void, unknown> {
	const resolved = resolveOptions(options);
	for await (const batch of generateFromResolved(resolved)) {
		yield batch as TOutput[];
	}
}

export async function generate<TOutput = FakeTimeSeriesData>(
	options: FakeTimeSeriesOptions<TOutput> = {},
): Promise<FakeTimeSeriesResult<TOutput>> {
	const resolved = resolveOptions(options);

	const allBatches: unknown[][] = [];
	// Track totalBatches with a dedicated counter rather than
	// `allBatches.length`, so the invariant doesn't silently break if a
	// future change stops pushing into `allBatches` (mirrors `toSink`'s
	// pattern, which supports `retainBatches: false`).
	let totalBatches = 0;
	let totalMessages = 0;

	for await (const batch of generateFromResolved(resolved)) {
		allBatches.push(batch);
		totalBatches++;
		totalMessages += batch.length;
	}

	return {
		batches: allBatches as TOutput[][],
		startTime: resolved.startDate,
		endTime: resolved.endDate,
		minInterval: resolved.minInt,
		maxInterval: resolved.maxInt,
		totalBatches,
		totalMessages,
	};
}

export interface FakeTimeSeriesToSinkOptions<TOutput = FakeTimeSeriesData>
	extends FakeTimeSeriesOptions<TOutput> {
	fetcher: (batch: TOutput[]) => Promise<Response>;
	concurrency?: number;
	onError?: (error: unknown) => void;
	/**
	 * If true (default), `toSink` accumulates every batch in memory and
	 * returns them in `FakeTimeSeriesResult.batches`. Set to `false` for
	 * very large datasets — the result will report counters only and
	 * `batches` will be an empty array.
	 */
	retainBatches?: boolean;
}

const DEFAULT_CONCURRENCY = 10;

const defaultSinkErrorHandler = (error: unknown): void => {
	// biome-ignore lint/suspicious/noConsole: library-level default for CLI/debug output
	console.error(
		error instanceof Error
			? `${error.name}: ${error.message}`
			: JSON.stringify(error, null, 2),
	);
};

/**
 * Returns a fresh copy of the `toSink` defaults with `endTime` set to the
 * current time. Prefer this over the deprecated
 * {@link defaultFakeTimeSeriesToSinkOptions} const, which leaves `endTime`
 * as `undefined` so that it can be resolved lazily at call time.
 */
export const createDefaultFakeTimeSeriesToSinkOptions = (): Omit<
	FakeTimeSeriesToSinkOptions,
	"fetcher"
> => ({
	...defaultOptions,
	endTime: new Date(),
	concurrency: DEFAULT_CONCURRENCY,
	onError: defaultSinkErrorHandler,
});

/**
 * Backwards-compatible default options snapshot. `endTime` is intentionally
 * `undefined` here — `toSink` resolves it lazily at call time. Consumers that
 * need a concrete `endTime` in the snapshot should call
 * {@link createDefaultFakeTimeSeriesToSinkOptions} instead.
 *
 * @deprecated Prefer {@link createDefaultFakeTimeSeriesToSinkOptions} for a
 * fresh snapshot, or pass explicit options to `toSink`.
 */
export const defaultFakeTimeSeriesToSinkOptions: Omit<
	FakeTimeSeriesToSinkOptions,
	"fetcher"
> = {
	...defaultOptions,
	concurrency: DEFAULT_CONCURRENCY,
	onError: defaultSinkErrorHandler,
};

// Cache the dynamic `p-limit` import at module scope so repeated `toSink`
// calls don't pay for it on every invocation.
let pLimitModulePromise: Promise<typeof import("p-limit").default> | undefined;
const getPLimit = (): Promise<typeof import("p-limit").default> => {
	if (!pLimitModulePromise) {
		pLimitModulePromise = import("p-limit").then((m) => m.default);
	}
	return pLimitModulePromise;
};

export async function toSink<TOutput = FakeTimeSeriesData>(
	options: FakeTimeSeriesToSinkOptions<TOutput>,
): Promise<FakeTimeSeriesResult<TOutput>> {
	if (typeof options.fetcher !== "function") {
		throw new Error("fetcher is required");
	}

	const concurrency = options.concurrency ?? DEFAULT_CONCURRENCY;
	if (!Number.isFinite(concurrency) || concurrency <= 0) {
		throw new Error(
			`concurrency must be a positive number (got ${concurrency})`,
		);
	}

	// Validate all time-series options BEFORE loading p-limit, so a bad
	// config fails fast without paying for a dynamic import.
	const resolved = resolveOptions(options);

	const onError = options.onError ?? defaultSinkErrorHandler;
	const retainBatches = options.retainBatches ?? true;
	// The public API types the fetcher as receiving TOutput[], but the
	// internal generator yields unknown[]. The cast is safe because
	// TOutput is exactly what `transform` produces (or FakeTimeSeriesData
	// when no transform is provided).
	const fetcher = options.fetcher as unknown as (
		batch: unknown[],
	) => Promise<Response>;

	const pLimit = await getPLimit();
	const limit = pLimit(concurrency);

	const allBatches: unknown[][] = [];
	let totalBatches = 0;
	let totalMessages = 0;
	const inflight: Promise<void>[] = [];

	const safeOnError = (error: unknown): void => {
		try {
			onError(error);
		} catch (handlerError) {
			// biome-ignore lint/suspicious/noConsole: last-resort diagnostic
			console.error("onError handler threw:", handlerError);
		}
	};

	// Stream batches from the generator; schedule each send as it arrives
	// so requests start firing before generation completes.
	for await (const batch of generateFromResolved(resolved)) {
		totalBatches++;
		totalMessages += batch.length;
		if (retainBatches) {
			allBatches.push(batch);
		}
		inflight.push(
			limit(async () => {
				try {
					const response = await fetcher(batch);
					if (!response.ok) {
						const body = await response.text().catch(() => "");
						safeOnError(
							new Error(`Sink responded with ${response.status}: ${body}`),
						);
					}
				} catch (error) {
					safeOnError(error);
				}
			}),
		);
	}

	await Promise.all(inflight);

	return {
		batches: allBatches as TOutput[][],
		startTime: resolved.startDate,
		endTime: resolved.endDate,
		minInterval: resolved.minInt,
		maxInterval: resolved.maxInt,
		totalBatches,
		totalMessages,
	};
}
