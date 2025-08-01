export const options = {
	shapes: {
		temperature: () => ({
			sensorId: Math.random().toString(36).substring(2, 15),
			value: Math.random() * 100,
		}),
	},
	concurrency: 10,
};
