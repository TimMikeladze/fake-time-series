// Fixture used by tests/cli.test.ts to exercise loadConfig's dynamic import
// path. This file is intentionally isolated from the repo's own
// fake-time-series.config.mjs so unrelated edits to that file don't break
// the CLI tests.
export const options = {
	maxBatchSize: 7,
	batchReverseProbability: 0.25,
	concurrency: 3,
	sinkUrl: "https://config.example.com/ingest",
	headers: {
		"X-Config-Header": "from-config",
	},
};
