import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { Command } from "commander";
import type {
	FakeTimeSeriesOptions,
	FakeTimeSeriesToSinkOptions,
	TransformFunction,
} from "./index";

const DEFAULT_CONFIG_FILES = [
	"fake-time-series.config.mjs",
	"fake-time-series.config.js",
];

// Tell vite's SSR transform to leave this dynamic import alone (it otherwise
// tries to resolve arbitrary file:// URLs at transform time and fails for
// paths that aren't known at build). At runtime, Node's native loader handles
// the file:// URL correctly.
const dynamicImport = (specifier: string): Promise<Record<string, unknown>> =>
	import(/* @vite-ignore */ specifier);

export type CliConfig = Partial<
	FakeTimeSeriesOptions &
		Omit<FakeTimeSeriesToSinkOptions, "fetcher"> & {
			headers?: Record<string, string>;
			sinkUrl?: string;
			transform?: TransformFunction;
		}
>;

/**
 * Pure path-resolution step for {@link loadConfig}. Decides which file on
 * disk should be loaded (if any) without actually importing it. Split out
 * so the existence/extension-fallback logic can be unit-tested without
 * going through the dynamic import machinery that vitest intercepts
 * in-process.
 *
 * - If `configPath` is given, try that path first. If it has no `.mjs`/`.js`
 *   extension, try appending each one. Throws if none of the candidates
 *   exist.
 * - If `configPath` is omitted, walk the default config filenames in `cwd`.
 *   Returns `null` if none exist — the CLI must work without a config file.
 */
export function resolveConfigPath(
	configPath: string | undefined,
	cwd: string = process.cwd(),
	existsSync: (p: string) => boolean = fs.existsSync,
): string | null {
	if (configPath) {
		const resolved = path.resolve(cwd, configPath);
		const hasExtension = resolved.endsWith(".mjs") || resolved.endsWith(".js");
		const candidates = hasExtension
			? [resolved]
			: [`${resolved}.mjs`, `${resolved}.js`];

		for (const candidate of candidates) {
			if (existsSync(candidate)) {
				return candidate;
			}
		}
		throw new Error(
			`Config file not found: ${configPath} (tried ${candidates.join(", ")})`,
		);
	}

	for (const defaultName of DEFAULT_CONFIG_FILES) {
		const candidate = path.resolve(cwd, defaultName);
		if (existsSync(candidate)) {
			return candidate;
		}
	}

	return null;
}

/**
 * Load a config file.
 *
 * - If `configPath` is given, require it to exist. Try the given path, then
 *   `.mjs` and `.js` extensions if no extension was specified.
 * - If `configPath` is omitted, look for the default config files in cwd.
 *   Return an empty object if none exist — the CLI must work without a
 *   config file.
 */
export async function loadConfig(configPath?: string): Promise<CliConfig> {
	const resolvedPath = resolveConfigPath(configPath);
	if (!resolvedPath) {
		return {};
	}
	const mod = await dynamicImport(pathToFileURL(resolvedPath).href);
	return (mod.options as CliConfig | undefined) ?? {};
}

// Accept `N` or `N.0+` only — no trailing characters, no scientific
// notation, no negative sign. This is stricter than `parseFloat`, which
// silently accepts "42abc" as 42 and "4e2" as 400.
const POSITIVE_INT_PATTERN = /^\d+(?:\.0+)?$/;

// Accept plain decimal numbers only (e.g. "0", "1", "0.5", "0.999"). No
// trailing characters, no scientific notation, no sign. Stricter than
// `parseFloat`, which accepts "0.5 xyz" as 0.5 via partial parse.
const DECIMAL_PATTERN = /^\d+(?:\.\d+)?$/;

/**
 * Commander parser for a positive integer flag. Accepts `"4"` and `"4.0"`
 * (both parse as `4`) and rejects anything else — `"4.5"`, `"abc"`, `"0"`,
 * `"-1"`, `"42abc"`, `"4e2"`, `"Infinity"`, etc.
 *
 * Throws a descriptive error for invalid input so it surfaces as a
 * commander validation error rather than a silent NaN or a silent
 * partial parse.
 */
export const parsePositiveInt =
	(name: string) =>
	(value: string): number => {
		const trimmed = value.trim();
		if (!POSITIVE_INT_PATTERN.test(trimmed)) {
			throw new Error(`${name} must be a positive integer (got "${value}")`);
		}
		const parsed = Number.parseFloat(trimmed);
		if (!Number.isFinite(parsed) || parsed <= 0) {
			throw new Error(`${name} must be a positive integer (got "${value}")`);
		}
		return parsed;
	};

/**
 * Commander parser for a probability flag in [0, 1]. Uses the same
 * regex-gated approach as {@link parsePositiveInt} so partial parses
 * (e.g. `"0.5 xyz"` → 0.5) and scientific notation are rejected rather
 * than silently accepted.
 */
export const parseProbability =
	(name: string) =>
	(value: string): number => {
		const trimmed = value.trim();
		if (!DECIMAL_PATTERN.test(trimmed)) {
			throw new Error(
				`${name} must be a number between 0 and 1 (got "${value}")`,
			);
		}
		const parsed = Number.parseFloat(trimmed);
		if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
			throw new Error(
				`${name} must be a number between 0 and 1 (got "${value}")`,
			);
		}
		return parsed;
	};

/**
 * Walk up from the script location to find the nearest `package.json`
 * that declares `fake-time-series` as a `bin` entry and return its
 * version. This works whether the package has been renamed by a fork
 * (as long as the `bin` entry is preserved) and whether the CLI is
 * invoked via a symlink, a direct path, or a bundled build.
 *
 * Returns `"unknown"` if no matching package.json is found — an
 * explicit sentinel is friendlier than a made-up `0.0.0`.
 */
export function readVersion(): string {
	try {
		const scriptPath = process.argv[1];
		if (!scriptPath) {
			return "unknown";
		}
		let dir: string;
		try {
			dir = path.dirname(fs.realpathSync(scriptPath));
		} catch {
			dir = path.dirname(scriptPath);
		}
		// Walk from the script's directory up to the filesystem root.
		// Using `parent === dir` as the terminator means there's no magic
		// depth limit — deeply nested monorepos still work.
		while (true) {
			const pkgPath = path.join(dir, "package.json");
			if (fs.existsSync(pkgPath)) {
				try {
					const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf-8")) as {
						name?: string;
						version?: string;
						bin?: string | Record<string, string>;
					};
					const declaresBin =
						pkg.bin != null &&
						(typeof pkg.bin === "string" ||
							Object.hasOwn(pkg.bin, "fake-time-series"));
					if (declaresBin && typeof pkg.version === "string") {
						return pkg.version;
					}
				} catch {
					// keep walking
				}
			}
			const parent = path.dirname(dir);
			if (parent === dir) {
				break;
			}
			dir = parent;
		}
	} catch {
		// fall through
	}
	return "unknown";
}

/**
 * Collect only the options explicitly set by the user on the command line
 * (or via env/config), excluding values left at their commander defaults.
 * This lets the config file supply a value when the CLI flag wasn't passed.
 */
export const collectExplicitOptions = (
	cmd: Command,
): Record<string, unknown> => {
	const result: Record<string, unknown> = {};
	for (const opt of cmd.options) {
		const attr = opt.attributeName();
		const source = cmd.getOptionValueSource(attr);
		if (source && source !== "default" && source !== "implied") {
			result[attr] = cmd.getOptionValue(attr);
		}
	}
	return result;
};
