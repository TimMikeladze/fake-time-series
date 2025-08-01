# ⏲️ fake-time-series

A flexible CLI tool and library for generating fake time series data. Perfect for testing, development, and demonstration purposes.

- ✅ Generate realistic time series data with customizable data shapes.
- ✅ Support for human-readable time ranges like "3 days ago" or "1 year ago" or "2024-6-30"
- ✅ Send data to HTTP endpoints.
- ✅ Fine-grained control over data generation with batch size and interval settings
- ✅ Built-in randomization features for more realistic data patterns.
- ✅ Simple configuration via JavaScript/ESM config files.

## 📡 Installation

```bash
npm install fake-time-series

yarn add fake-time-series

pnpm add fake-time-series
```

> 👋 Hello there! Follow me [@linesofcode](https://twitter.com/linesofcode) or visit [linesofcode.dev](https://linesofcode.dev) for more cool projects like this one.

## 🚀 Getting Started

```bash
fake-time-series generate --startTime "1 week ago" --endTime "1 day ago"
```

Send generated data to an endpoint:
```bash
fake-time-series send --sink-url "http://localhost:3000/api/ingest" --startTime "3 months ago" --endTime "1 month ago"
```

## 🛠️ Configuration

### Config File

The tool supports configuration through a JavaScript/ESM config file. By default, it looks for `fake-time-series.config.mjs` in the current directory.

A sample config file:

```javascript
// fake-time-series.config.mjs
export const options = {
  startTime: "3 weeks ago",
  minInterval: "5s",
  maxInterval: "30s",
  maxBatchSize: 20,
  sinkUrl: "http://localhost:3000/api/ingest",
  concurrency: 5,
  headers: {
    "Authorization": "Bearer your-token",
    "Content-Type": "application/json"
  },
  shapes: {
		temperature: () => ({
			sensorId: Math.random().toString(36).substring(2, 15),
			value: Math.random() * 100,
		}),
	},
};
```

### Shapes 

Shapes are functions that return a data point. They can be used to generate realistic data for your specific use case.

Each shape function is identified by a key in the `shapes` object in the config file. Each shape function accepts the current timestamp and returns a data point, this can be a single value or a more complex object.

For example, the following shape function generates a random temperature value between 0 and 100 for a given timestamp:

```javascript
// fake-time-series.config.mjs
export const config = {
	shapes: {
		temperature: (timestamp) => ({
			sensorId: Math.random().toString(36).substring(2, 15),
			value: Math.random() * 100,
		}),
	},
};
```

### Transform Function

You can add a `transform` function to modify the structure of each generated data point. This is useful when you need to convert the default format to match your target system's expected format.

The transform function receives a data point with the structure:
```javascript
{
  timestamp: number,
  key: string,
  data: Record<string, unknown>
}
```

And should return a new object with your desired structure.

Example:
```javascript
// fake-time-series.config.mjs
export const options = {
  shapes: {
    temperature: () => ({
      sensorId: Math.random().toString(36).substring(2, 15),
      value: Math.random() * 100,
    }),
  },
  transform: (dataPoint) => ({
    partitionKey: dataPoint.key,
    timestamp: dataPoint.timestamp,
    data: {
      value: dataPoint.data.value || "",
    },
  }),
};
```

This will transform each data point from:
```javascript
{
  timestamp: 1753950933982,
  key: "temperature",
  data: {
    sensorId: "138lnj9gs7kq",
    value: 12.1311483084942,
  },
}
```

To:
```javascript
{
  partitionKey: "138lnj9gs7kq",
  timestamp: 1753950933982,
  data: {
    value: 12.1311483084942,
  },
}
```

Specify a custom config path:
```bash
fake-time-series generate --config ./custom-config.mjs
```

### Time Formats

The tool supports various time formats for `startTime` and `endTime`:

#### Relative Times
- Simple offsets:  `-1 day`, `-30 minutes`, `-12 hours`, `-1 week`, `1 day ago`, `30 minutes ago`
- Multiple units: `-1 day 6 hours`, `-2 weeks 3 days`, `1 week 2 days ago`
- Special keywords: `now`, `today`, `yesterday`

#### Absolute Times
- ISO 8601: `2024-01-15T10:30:00Z`
- Date strings: `2024-01-15`, `15-01-2024`
- Time with timezone: `2024-01-15T10:30:00-05:00`

#### Interval Formats
For `minInterval` and `maxInterval`:
- Seconds: `5s`, `30s`
- Minutes: `1m`, `5m`
- Hours: `1h`, `12h`
- Days: `1d`
- Milliseconds: `1234567890`

## 🔧 Options

| Option | Description | Default |
|--------|-------------|---------|
| `startTime` | Start time of the series | `-1 day` |
| `endTime` | End time of the series | `now` |
| `minInterval` | Minimum interval between points | `1s` |
| `maxInterval` | Maximum interval between points | `10s` |
| `maxBatchSize` | Maximum points per batch | `10` |
| `batchSizeRandomization` | Enable random batch sizes | `true` |
| `intervalRandomization` | Enable random intervals | `true` |
| `batchReverseProbability` | Chance to reverse batch order | `0.5` |
| `batchShuffleProbability` | Chance to shuffle batch | `0.4` |
| `intervalSkewProbability` | Chance of interval skewing | `0.8` |
| `transform` | Function to transform data points | `undefined` |
| `concurrency` | Max concurrent requests (send only) | `10` |
| `sinkUrl` | Endpoint URL (send only) | required |
| `headers` | Request headers (send only) | `{"Content-Type": "application/json"}` |

## Examples

Generate data for the last 3 days:
```bash
fake-time-series generate --startTime "-3 days"
```

Generate data with specific intervals:
```bash
fake-time-series generate --minInterval "30s" --maxInterval "5m"
```

Send data with custom headers:
```bash
fake-time-series send \
  --sink-url "http://localhost:3000/api/ingest" \
  --headers '{"Authorization": "Bearer token123"}'
```
