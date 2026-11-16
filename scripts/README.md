# Scripts

Reproducible DuckDB pipeline for the UMDAC Datathon 2026 / Swift payment dataset.

| Script | Purpose |
|---|---|
| [`prepare_data.sql`](prepare_data.sql) | Parse and clean the source CSV, write the full Parquet, the 5% account sample, anomaly extract, preview and all public aggregates |
| [`verify_data.sql`](verify_data.sql) | Run 34 integrity checks and compare the sample with the full data; writes `qa_checks.csv` and `qa_sample_vs_full.csv` |
| [`make_manifest.mjs`](make_manifest.mjs) | Write SHA-256 / size / row-count manifests for the public and local bundles |

---

## Prerequisites

- **DuckDB v1.5.x** (tested with `v1.5.6 (Variegata)`).
- **Node.js 20+** only if you want to regenerate the manifests (tested with v26.13.1).
- The official source file in the repository root:
  `swift_umdac_datathon_2026_fictitious_payment_data.csv` (4.95 GB, not committed).
- About **3 GB free disk** and 8 GB+ RAM for a full rebuild.

Run everything from the repository root:

```bash
duckdb -f scripts/prepare_data.sql
duckdb -f scripts/verify_data.sql
node scripts/make_manifest.mjs
```

`prepare_data.sql` prints a five-value summary (`full_rows`, `sample_rows`,
`accounts`, `sampled_accounts`, `anomalies`). Expected values:

```
10,000,000 | 498,645 | 391,132 | 19,670 | 7,006
```

---

## Outputs

### Public (committed)

| Path | Contents |
|---|---|
| `data/aggregates/*.csv` | 23 full-data aggregate tables |
| `data/aggregates/qa_checks.csv` | 34 integrity checks |
| `data/aggregates/qa_sample_vs_full.csv` | 27 sample-vs-full metrics |
| `data/manifest.json` | Sizes, SHA-256 and row counts for every public file |

See [`../data/README.md`](../data/README.md) for the column-level catalogue and
[`../data/QA.md`](../data/QA.md) for the QA report.

### Local (gitignored, row-level)

| Path | Rows | Approx. size | Contents |
|---|---:|---:|---|
| `data_local/swift_umdac_2026_clean_full.parquet` | 10,000,000 | 1.34 GB | Cleaned full dataset: 20 source columns + derived columns |
| `data_local/swift_umdac_2026_clean_sample_5pct.csv.gz` | 498,645 | 73 MB | Raw-like sample, exact 20 source columns, header included |
| `data_local/swift_umdac_2026_clean_sample_5pct.parquet` | 498,645 | 54 MB | Typed sample with derived columns |
| `data_local/swift_umdac_2026_anomalies.csv.gz` | 7,006 | 0.8 MB | Every row with a non-blank `ANOMALY_TYPE` |
| `data_local/preview_100.csv` | 100 | 49 KB | Small mixed preview |
| `data_local/country_tz.csv` | 228 | 4 KB | ISO2 → IANA timezone map used by the local-hour aggregate |
| `data_local/manifest_local.json` | — | — | Sizes, SHA-256 and row counts for the six files above |

The row-level bundle is **not** for publication during the competition. See
[`../README.md`](../README.md) for the competition notice.

---

## Design decisions

### Parsing the CSV

The source file is malformed by design. Record 1 is a header, and record 2 (the
first payment) begins with the 506-character disclaimer as its first field. The
script therefore uses:

```sql
read_csv(..., header = false, skip = 1, all_varchar = true)
```

and drops the leading `column00`. Reading with `header = true` would either shift
the first payment or fail on the embedded newlines in `ORDERING_DETAILS`.

### Cleaning without deleting signal

- **No de-duplication.** `DUPLICATE_PAYMENT` groups are intentional anomalies.
- **No amount capping.** The maximum (`15,664,132,213.09`) is a deliberate
  fat-finger pattern; winsorising would erase it.
- **No path repair.** `BIC8_PATH` endpoints are replaced with probability ≈ 5% by
  design. The authoritative BICs remain in the dedicated columns.
- **No row filtering.** The only nulls introduced are `NULL` for blank
  `ANOMALY_TYPE` values.

### Derived columns

| Column | Rule |
|---|---|
| `EVENT_TS_UTC` | `make_timestamp(EPOCH_SECONDS * 1000000)` |
| `ORDERING_COUNTRY` / `BENEFICIARY_COUNTRY` | BIC characters 5–6 |
| `PATH_START_MATCH` / `PATH_END_MATCH` | First / last `BIC8_PATH` token = ordering / beneficiary BIC |
| `PATH_GAP_COUNT` | Count of `...` tokens: `(length(path) − length(replace(path,'...',''))) / 3` |
| `IS_LOCAL_CURRENCY` | `CURRENCY_CODE` = modal currency of the ordering country |

`local_ccy` is derived once as the per-country modal currency and joined back.
Modal currency, not a hard-coded ISO table, keeps the pipeline self-contained.

### Local-hour timezone map

`agg_time_hour_local.csv` needs a country clock. The pipeline embeds a hard-coded
ISO 3166-1 alpha-2 → IANA map (`country_tz`, 228 entries) using each country's
capital / main financial centre; it is also written to `data_local/country_tz.csv`
so `verify_data.sql` can check coverage. Timestamps are cast through UTC first
(`(EVENT_TS_UTC AT TIME ZONE 'UTC') AT TIME ZONE <tz>`) so the DuckDB ICU extension
applies DST correctly, and `SET TimeZone = 'UTC'` pins the session zone so results
do not depend on the host locale.

### Sampling

- Unit: **ordering account** (`ORDERING_ACCOUNT`, non-empty only).
- Strata: **ordering country**.
- Allocation: `ceil(0.05 × n_accounts_in_country)`, so every country contributes at
  least one account.
- Selection: `row_number() OVER (PARTITION BY ordering_country ORDER BY hash(account))`.
- Keeps **all payments** of each selected account.

Result: 19,670 accounts, 498,645 rows (4.9864%), all 228 ordering countries.
Keeping whole accounts preserves heavy-tailed sender behaviour and duplicate
groups; country stratification preserves geographic coverage. It is a
deliberately *descriptive* sample, not a uniform row sample.

All committed aggregates use the **full 10M rows**, never the sample.

### QA tolerances

The only non-exact check is `path_gap_formula_max_deviation_pp` (tolerance 0.2 pp).
Observed gap rates for 2–6 participants match `1 − 0.95^(participants−1)` within
0.095 pp. Other checks are exact or use narrow ranges tied to known invariants
(e.g. local-currency share 89–90%, endpoint matches 94.5–95.5%).

---

## Implementation notes

- **Reserved identifiers.** `full` and `sample` are reserved in DuckDB 1.5.x, so the
  views are named `payments` and `sample_rows`.
- **`regexp_count` is unavailable.** Gap counting uses the
  `length − length(replace(...))` identity.
- **Spill directory.** `temp_directory = 'data_local/duckdb_tmp'`; the directory is
  created automatically by DuckDB and is gitignored. It can be deleted between runs.
- **Threads / ordering.** `threads = 8` and `preserve_insertion_order = false` keep
  the sort-heavy window functions within memory. Lower `threads` on a small
  machine; raise it on a large one.
- **Row order in outputs.** The sample CSV and Parquet are ordered by
  `EPOCH_SECONDS, ORDERING_ACCOUNT`; every aggregate `ORDER BY` ends in a unique
  key (country, currency, party pair, …) so tied sort values cannot reorder between
  runs. Hash-based selection is deterministic for a given DuckDB build, so reruns
  are byte-reproducible.
- **Regenerating manifests.** `make_manifest.mjs` reads row counts with DuckDB, so
  it must run *after* `prepare_data.sql` and `verify_data.sql`. Set `DUCKDB_BIN` if
  `duckdb` is not on `PATH`.

## Runtime

On a typical 8-core laptop:

| Step | Time |
|---|---|
| `prepare_data.sql` | 3–8 minutes (dominated by the 4.95 GB CSV scan and Parquet write) |
| `verify_data.sql` | 1–3 minutes |
| `make_manifest.mjs` | under 1 minute (SHA-256 of 1.34 GB + one DuckDB count query) |

The pipeline is single-pass for parsing; the heavy aggregates rescan the full
Parquet several times, so enough RAM/disk spill space matters more than CPU.
