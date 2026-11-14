#!/usr/bin/env node
// ============================================================================
// UMDAC Datathon 2026 / Swift -- build file manifests
// ============================================================================
// Usage (from the repository root):
//   node scripts/make_manifest.mjs
//
// Writes:
//   data/manifest.json          public files only (aggregates + QA)
//   data_local/manifest_local.json  all local row-level outputs
//
// Each entry records path, size, SHA-256 and row count. Row counts are read
// with DuckDB so compressed CSV and Parquet are handled exactly.
// ============================================================================

import { createHash } from "node:crypto";
import { createReadStream, existsSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
const DUCKDB = process.env.DUCKDB_BIN || "duckdb";

const SKIP_NAMES = new Set(["manifest.json", "manifest_local.json", ".DS_Store", "Thumbs.db"]);
const SKIP_DIRS = new Set(["duckdb_tmp", "node_modules", ".git"]);

function walk(dir) {
  if (!existsSync(dir)) return [];
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!SKIP_DIRS.has(entry.name)) out.push(...walk(path.join(dir, entry.name)));
    } else if (entry.isFile() && !SKIP_NAMES.has(entry.name)) {
      out.push(path.join(dir, entry.name));
    }
  }
  return out;
}

function relPosix(absPath) {
  return path.relative(ROOT, absPath).split(path.sep).join("/");
}

async function sha256(file) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(file)) hash.update(chunk);
  return hash.digest("hex");
}

function sqlString(value) {
  return "'" + value.replace(/'/g, "''") + "'";
}

function readerFor(relPath) {
  const lower = relPath.toLowerCase();
  if (lower.endsWith(".parquet")) return `read_parquet(${sqlString(relPath)})`;
  if (lower.endsWith(".csv") || lower.endsWith(".csv.gz") || lower.endsWith(".tsv") || lower.endsWith(".tsv.gz")) {
    return `read_csv_auto(${sqlString(relPath)}, header = true)`;
  }
  return null;
}

function readRowCounts(relPaths) {
  const parts = relPaths
    .map((rel) => {
      const reader = readerFor(rel);
      if (!reader) return null;
      return `SELECT ${sqlString(rel)} AS path, count(*) AS rows FROM ${reader}`;
    })
    .filter(Boolean);

  if (parts.length === 0) return new Map();

  const sql = parts.join("\nUNION ALL\n");
  const stdout = execFileSync(DUCKDB, ["-json", "-c", sql], {
    cwd: ROOT,
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
  });
  const rows = JSON.parse(stdout);
  return new Map(rows.map((row) => [row.path, Number(row.rows)]));
}

function duckdbVersion() {
  try {
    return execFileSync(DUCKDB, ["--version"], { encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

async function buildEntries(absPaths) {
  const rels = absPaths.map(relPosix).sort();
  const rowCounts = readRowCounts(rels);
  const entries = [];
  for (const rel of rels) {
    const abs = path.join(ROOT, rel);
    entries.push({
      path: rel,
      bytes: statSync(abs).size,
      sha256: await sha256(abs),
      rows: rowCounts.has(rel) ? rowCounts.get(rel) : null,
    });
  }
  return entries;
}

function totals(entries) {
  return {
    files: entries.length,
    bytes: entries.reduce((sum, entry) => sum + entry.bytes, 0),
  };
}

const source = {
  file: "swift_umdac_datathon_2026_fictitious_payment_data.csv",
  rows: 10_000_000,
  fields: 21,
  bytes: 4_952_194_328,
  time_span_utc: {
    start: "2023-04-30T11:22:26Z",
    end: "2023-07-31T09:56:01Z",
  },
  ordering_countries: 228,
  beneficiary_countries: 229,
  currencies: 155,
  ordering_bics: 8_392,
  beneficiary_bics: 8_266,
  anomaly_rows: 7_006,
  disclaimer: "See DISCLAIMER.txt. Fictitious data, (c) Swift 2026.",
};

const sampling = {
  unit: "ordering_account",
  fraction: 0.05,
  strata: "ordering_country",
  allocation: "ceil(0.05 * n_accounts_per_country)",
  selection: "row_number() OVER (PARTITION BY ordering_country ORDER BY hash(ordering_account))",
  keeps: "all payments of each selected account",
  accounts: 19_670,
  rows: 498_645,
  fraction_of_rows_pct: 4.9864,
  ordering_countries_covered: 228,
};

const software = {
  duckdb: duckdbVersion(),
  node: process.version,
};

const generatedAt = new Date().toISOString();

const publicEntries = await buildEntries(walk(path.join(ROOT, "data")));
const localEntries = await buildEntries(walk(path.join(ROOT, "data_local")));

const publicManifest = {
  project: "UMDAC Datathon 2026 - Swift",
  bundle: "public aggregates and code",
  description:
    "Full-data aggregate tables and QA outputs. Contains no row-level payment records.",
  row_level: false,
  generated_at: generatedAt,
  source,
  sampling,
  software,
  totals: totals(publicEntries),
  files: publicEntries,
};

const localManifest = {
  project: "UMDAC Datathon 2026 - Swift",
  bundle: "local row-level outputs (not for publication)",
  description:
    "Cleaned full dataset, 5% account sample, anomaly extract and preview. Do not publish during the competition.",
  row_level: true,
  generated_at: generatedAt,
  source,
  sampling,
  software,
  totals: totals(localEntries),
  files: localEntries,
};

writeFileSync(
  path.join(ROOT, "data", "manifest.json"),
  JSON.stringify(publicManifest, null, 2) + "\n",
  "utf8",
);
writeFileSync(
  path.join(ROOT, "data_local", "manifest_local.json"),
  JSON.stringify(localManifest, null, 2) + "\n",
  "utf8",
);

console.log(
  `Wrote data/manifest.json (${publicManifest.totals.files} files, ${publicManifest.totals.bytes} bytes)`,
);
console.log(
  `Wrote data_local/manifest_local.json (${localManifest.totals.files} files, ${localManifest.totals.bytes} bytes)`,
);
