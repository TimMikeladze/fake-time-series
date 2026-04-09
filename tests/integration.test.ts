import {
	createServer,
	type IncomingMessage,
	type Server,
	type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it } from "vitest";
import { buildProgram } from "../src/cli";
import { type FakeTimeSeriesData, toSink } from "../src/index";

/**
 * Integration tests that exercise the real `fetch` path against an
 * in-process HTTP listener on `127.0.0.1:0`.
 *
 * These complement the spy-based unit tests in `cli.test.ts` by catching
 * regressions that only manifest when a real socket is involved:
 *   - body serialization (JSON.stringify → wire bytes → server)
 *   - Content-Type / Content-Length set by Node's fetch
 *   - Non-2xx responses other than 500 (401, 429)
 *   - Connection-refused / dead server failures
 *   - Concurrency cap observed by a real slow listener
 *   - CLI `send` command end-to-end
 */

interface RecordedRequest {
	method: string;
	url: string;
	headers: Record<string, string | string[] | undefined>;
	body: string;
}

const readBody = (req: IncomingMessage): Promise<string> =>
	new Promise((resolve, reject) => {
		const chunks: Buffer[] = [];
		req.on("data", (chunk: Buffer) => chunks.push(chunk));
		req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
		req.on("error", reject);
	});

type Handler = (
	req: IncomingMessage,
	res: ServerResponse,
	recorded: RecordedRequest,
) => Promise<void> | void;

const defaultHandler: Handler = (_req, res) => {
	res.statusCode = 200;
	res.end();
};

interface TestServer {
	url: string;
	requests: RecordedRequest[];
	close: () => Promise<void>;
}

const startTestServer = async (
	handler: Handler = defaultHandler,
): Promise<TestServer> => {
	const requests: RecordedRequest[] = [];

	const server: Server = createServer(async (req, res) => {
		try {
			const body = await readBody(req);
			const recorded: RecordedRequest = {
				method: req.method ?? "",
				url: req.url ?? "",
				headers: req.headers,
				body,
			};
			requests.push(recorded);
			await handler(req, res, recorded);
			if (!res.writableEnded) {
				res.end();
			}
		} catch (err) {
			if (!res.writableEnded) {
				res.statusCode = 500;
				res.end(err instanceof Error ? err.message : String(err));
			}
		}
	});

	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const { port } = server.address() as AddressInfo;

	return {
		url: `http://127.0.0.1:${port}`,
		requests,
		close: () =>
			new Promise<void>((resolve, reject) => {
				server.close((err) => (err ? reject(err) : resolve()));
			}),
	};
};

const withServer = async (
	fn: (server: TestServer) => Promise<void>,
	handler?: Handler,
): Promise<void> => {
	const server = await startTestServer(handler);
	try {
		await fn(server);
	} finally {
		await server.close();
	}
};

const makeJsonPoster =
	(
		url: string,
		extraHeaders: Record<string, string> = {},
	): ((batch: FakeTimeSeriesData[]) => Promise<Response>) =>
	async (batch) =>
		fetch(url, {
			method: "POST",
			headers: { "Content-Type": "application/json", ...extraHeaders },
			body: JSON.stringify(batch),
		});

const constStatus =
	(status: number, body = ""): Handler =>
	(_req, res) => {
		res.statusCode = status;
		res.end(body);
	};

describe("toSink over real HTTP", () => {
	it("POSTs every batch with JSON body, content-type, content-length, and custom headers", async () => {
		await withServer(async (server) => {
			const errors: unknown[] = [];
			const result = await toSink({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:30Z",
				minInterval: "10s",
				maxInterval: "10s",
				batchSizeRandomization: false,
				intervalRandomization: false,
				batchReverseProbability: 0,
				batchShuffleProbability: 0,
				maxBatchSize: 2,
				concurrency: 1,
				fetcher: makeJsonPoster(server.url, { "X-Batch-Test": "real" }),
				onError: (e) => {
					errors.push(e);
				},
			});

			expect(errors).toEqual([]);
			expect(server.requests.length).toBe(result.totalBatches);
			expect(server.requests.length).toBeGreaterThan(0);

			// With concurrency: 1 and randomization disabled, wire order matches
			// result.batches order — so we can assert exact body equality and
			// catch any future regression where the body serialization or
			// the batch-to-fetcher handoff silently mangles data.
			for (let i = 0; i < server.requests.length; i++) {
				const req = server.requests[i];
				expect(req.method).toBe("POST");
				expect(req.headers["content-type"]).toBe("application/json");
				expect(req.headers["x-batch-test"]).toBe("real");
				// Node's fetch sets Content-Length for a JSON-string body. If a
				// future change swaps to a streaming body this assertion will
				// fail, surfacing the behavior change loudly.
				expect(req.headers["content-length"]).toBeDefined();
				expect(Number(req.headers["content-length"])).toBeGreaterThan(0);

				const wireBatch = JSON.parse(req.body) as FakeTimeSeriesData[];
				expect(wireBatch).toEqual(result.batches[i]);
			}

			// Defence in depth: total datapoints on the wire matches the
			// library's own counter.
			const wireCount = server.requests.reduce(
				(sum, r) => sum + (JSON.parse(r.body) as unknown[]).length,
				0,
			);
			expect(wireCount).toBe(result.totalMessages);
		});
	});

	it("routes 401 responses to onError with status and body", async () => {
		const errors: unknown[] = [];
		await withServer(
			async (server) => {
				await toSink({
					startTime: "2024-01-01T00:00:00Z",
					endTime: "2024-01-01T00:00:20Z",
					minInterval: "10s",
					maxInterval: "10s",
					fetcher: makeJsonPoster(server.url),
					onError: (e) => {
						errors.push(e);
					},
				});
			},
			constStatus(401, "unauthorized"),
		);

		expect(errors.length).toBeGreaterThan(0);
		for (const err of errors) {
			expect(err).toBeInstanceOf(Error);
			const message = (err as Error).message;
			expect(message).toContain("401");
			expect(message).toContain("unauthorized");
		}
	});

	it("routes 429 rate-limit responses to onError", async () => {
		const errors: unknown[] = [];
		await withServer(
			async (server) => {
				await toSink({
					startTime: "2024-01-01T00:00:00Z",
					endTime: "2024-01-01T00:00:20Z",
					minInterval: "10s",
					maxInterval: "10s",
					fetcher: makeJsonPoster(server.url),
					onError: (e) => {
						errors.push(e);
					},
				});
			},
			constStatus(429, "slow down"),
		);

		expect(errors.length).toBeGreaterThan(0);
		for (const err of errors) {
			expect(err).toBeInstanceOf(Error);
			expect((err as Error).message).toContain("429");
		}
	});

	it("routes connection-refused (dead server) failures to onError without crashing", async () => {
		// Start a server to get a guaranteed-free local port, then close it
		// before running toSink. Any fetch to that port now fails at the
		// socket level — the real-world "sink URL wrong / server down" case.
		const server = await startTestServer();
		const deadUrl = server.url;
		await server.close();

		const errors: unknown[] = [];
		const result = await toSink({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:20Z",
			minInterval: "10s",
			maxInterval: "10s",
			fetcher: makeJsonPoster(deadUrl),
			onError: (e) => {
				errors.push(e);
			},
		});

		expect(errors.length).toBeGreaterThan(0);
		for (const err of errors) {
			// Node's fetch surfaces socket failures as a TypeError ("fetch
			// failed") with a `cause` chain. The important contract is:
			// onError receives an Error and toSink does NOT crash.
			expect(err).toBeInstanceOf(Error);
		}
		// toSink still returned a result with accurate counters even though
		// every batch failed on the wire.
		expect(result.totalBatches).toBeGreaterThan(0);
	});

	it("roundtrips a transform function's Unicode output through a real server", async () => {
		// Two gaps closed by this single test:
		//   1. No integration test ran the `transform` code path against a
		//      real socket — a regression where transform output was
		//      dropped or mutated post-fetch would have slipped through.
		//   2. Every previous body assertion used ASCII-only default shapes,
		//      so UTF-8 encoding/decoding across the wire was unverified.
		interface Metric {
			kind: "metric";
			ts: number;
			label: string;
			emoji: string;
			japanese: string;
		}

		await withServer(async (server) => {
			const result = await toSink<Metric>({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:20Z",
				minInterval: "10s",
				maxInterval: "10s",
				batchSizeRandomization: false,
				intervalRandomization: false,
				batchReverseProbability: 0,
				batchShuffleProbability: 0,
				maxBatchSize: 1,
				concurrency: 1,
				transform: (dp) => ({
					kind: "metric" as const,
					ts: dp.timestamp,
					label: `sample-${dp.key}`,
					emoji: "🎉",
					japanese: "日本語",
				}),
				fetcher: async (batch) =>
					fetch(server.url, {
						method: "POST",
						headers: { "Content-Type": "application/json; charset=utf-8" },
						body: JSON.stringify(batch),
					}),
			});

			expect(server.requests.length).toBe(result.totalBatches);
			expect(server.requests.length).toBeGreaterThan(0);

			for (let i = 0; i < server.requests.length; i++) {
				const req = server.requests[i];
				const contentType = req.headers["content-type"];
				expect(contentType).toBe("application/json; charset=utf-8");

				const body = JSON.parse(req.body) as Metric[];
				// Deep-equal the wire body against the library's own record —
				// any mangling (lost transform output, corrupted Unicode, lost
				// field, wrong type) will fail this assertion loudly.
				expect(body).toEqual(result.batches[i]);

				// Belt-and-braces: explicitly verify the Unicode code points
				// made it across intact. JSON.parse would happily decode
				// mojibake into new strings, so equality on the exact chars
				// is the real test of UTF-8 correctness end-to-end.
				for (const metric of body) {
					expect(metric.kind).toBe("metric");
					expect(metric.emoji).toBe("🎉");
					expect(metric.japanese).toBe("日本語");
					expect(typeof metric.ts).toBe("number");
					// Transform output replaces the default FakeTimeSeriesData
					// shape — `key` should not be present on the wire.
					expect((metric as unknown as { key?: string }).key).toBeUndefined();
				}
			}
		});
	});

	it("follows 3xx redirects and delivers the batch to the final destination", async () => {
		// Two real servers: A returns 307 with a Location header pointing at
		// B. Node's fetch follows redirects by default, so B should be the
		// one that actually receives the POST body.
		const destination = await startTestServer();
		const redirectHandler: Handler = (_req, res) => {
			res.statusCode = 307;
			res.setHeader("Location", destination.url);
			res.end();
		};
		const origin = await startTestServer(redirectHandler);

		try {
			const errors: unknown[] = [];
			await toSink({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:20Z",
				minInterval: "10s",
				maxInterval: "10s",
				batchSizeRandomization: false,
				maxBatchSize: 1,
				fetcher: makeJsonPoster(origin.url),
				onError: (e) => {
					errors.push(e);
				},
			});

			expect(errors).toEqual([]);
			// Node's fetch actually hits the origin server first (to see the
			// 307), then follows to the destination. Both will have recorded
			// the request; the important contract is that the destination —
			// the final hop — received the body intact.
			expect(destination.requests.length).toBeGreaterThan(0);
			for (const req of destination.requests) {
				expect(req.method).toBe("POST");
				const body = JSON.parse(req.body) as FakeTimeSeriesData[];
				expect(Array.isArray(body)).toBe(true);
				expect(body.length).toBe(1);
				expect(typeof body[0].timestamp).toBe("number");
			}
		} finally {
			await origin.close();
			await destination.close();
		}
	});

	it("routes DNS resolution failures (.invalid TLD) to onError", async () => {
		// `.invalid` is reserved by RFC 2606 and will never resolve, giving
		// us a DNS-level failure that's distinct from the connection-refused
		// case above. Both should flow cleanly through onError.
		const errors: unknown[] = [];
		const result = await toSink({
			startTime: "2024-01-01T00:00:00Z",
			endTime: "2024-01-01T00:00:20Z",
			minInterval: "10s",
			maxInterval: "10s",
			fetcher: makeJsonPoster(
				"http://fake-time-series-nonexistent-host.invalid",
			),
			onError: (e) => {
				errors.push(e);
			},
		});

		expect(errors.length).toBeGreaterThan(0);
		for (const err of errors) {
			expect(err).toBeInstanceOf(Error);
		}
		expect(result.totalBatches).toBeGreaterThan(0);
	});

	it("respects concurrency cap against a slow real listener", async () => {
		let active = 0;
		let maxActive = 0;
		const slowHandler: Handler = async (_req, res) => {
			active++;
			maxActive = Math.max(maxActive, active);
			await new Promise((resolve) => setTimeout(resolve, 20));
			active--;
			res.statusCode = 200;
			res.end();
		};

		await withServer(async (server) => {
			await toSink({
				startTime: "2024-01-01T00:00:00Z",
				endTime: "2024-01-01T00:00:30Z",
				minInterval: "5s",
				maxInterval: "5s",
				batchSizeRandomization: false,
				maxBatchSize: 1,
				fetcher: makeJsonPoster(server.url),
				concurrency: 2,
			});
		}, slowHandler);

		expect(maxActive).toBeLessThanOrEqual(2);
		expect(maxActive).toBeGreaterThanOrEqual(1);
	});
});

describe("CLI send command over real HTTP", () => {
	it("POSTs batches to a real listener with the expected URL, headers, and body shape", async () => {
		await withServer(async (server) => {
			const logs: string[] = [];
			const errors: unknown[] = [];
			const batchErrors: unknown[] = [];

			const program = buildProgram({
				log: (msg) => {
					logs.push(msg);
				},
				onError: (err) => {
					errors.push(err);
					throw err;
				},
				sinkBatchErrorHandler: (err) => {
					batchErrors.push(err);
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
					server.url,
					"--startTime",
					"2024-01-01T00:00:00Z",
					"--endTime",
					"2024-01-01T00:00:20Z",
					"--minInterval",
					"10s",
					"--maxInterval",
					"10s",
					"--no-batchSizeRandomization",
					"--maxBatchSize",
					"1",
					"--concurrency",
					"1",
					"--headers",
					'{"X-Integration":"cli-real"}',
				],
				{ from: "node" },
			);

			expect(errors).toEqual([]);
			expect(batchErrors).toEqual([]);
			expect(server.requests.length).toBeGreaterThan(0);

			for (const req of server.requests) {
				expect(req.method).toBe("POST");
				expect(req.headers["content-type"]).toBe("application/json");
				expect(req.headers["x-integration"]).toBe("cli-real");
				expect(req.headers["content-length"]).toBeDefined();

				const body = JSON.parse(req.body) as FakeTimeSeriesData[];
				expect(Array.isArray(body)).toBe(true);
				expect(body.length).toBe(1);
				const [dp] = body;
				expect(typeof dp.timestamp).toBe("number");
				expect(typeof dp.key).toBe("string");
				expect(dp.data).toBeDefined();
			}
		});
	});

	it("surfaces real 4xx responses through the injected batch error handler", async () => {
		const batchErrors: unknown[] = [];

		await withServer(
			async (server) => {
				const program = buildProgram({
					log: () => undefined,
					onError: (err) => {
						throw err;
					},
					sinkBatchErrorHandler: (err) => {
						batchErrors.push(err);
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
						server.url,
						"--startTime",
						"2024-01-01T00:00:00Z",
						"--endTime",
						"2024-01-01T00:00:20Z",
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
			},
			constStatus(401, "nope"),
		);

		expect(batchErrors.length).toBeGreaterThan(0);
		for (const err of batchErrors) {
			expect(err).toBeInstanceOf(Error);
			expect((err as Error).message).toContain("401");
		}
	});
});
