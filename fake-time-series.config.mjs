// This file is auto-loaded by the `fake-time-series` CLI when it runs from
// the project root (or when explicitly passed via `--config`). It illustrates
// the `shapes` option: multiple named shape functions the generator chooses
// between at random.
export const options = {
	shapes: {
		// The built-in default shape, kept so running `fake-time-series generate`
		// from the project root still produces the expected `default` key in
		// some output points.
		default: () => ({
			value: Math.random(),
		}),
		temperature: () => ({
			sensorId: Math.random().toString(36).substring(2, 15),
			value: Math.random() * 100,
		}),
	},
	concurrency: 10,
};
