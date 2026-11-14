# UMDAC Datathon 2026 — Swift

Reproducible data preparation and quality assurance for the UMDAC Datathon 2026
Swift case study, built with DuckDB.

The official dataset is a **fictitious** payment simulator containing 10,000,000
rows and 21 fields (a disclaimer plus 20 payment fields). This repository contains
the pipeline that parses, cleans, samples, aggregates and validates it, together
with the **full-data aggregate outputs** and a QA report.

> **Competition notice.** The preliminary round is in progress. This public
> repository intentionally excludes row-level payment data and private analysis
> notes. Do not redistribute the source dataset, row-level extracts or analysis
> artefacts except as permitted by the UMDAC Datathon 2026 Rules and Regulations.
> See [`DISCLAIMER.txt`](DISCLAIMER.txt).

---

## Repository layout

```
.
├── data/
│   ├── aggregates/            # 21 public CSV tables (19 aggregates + 2 QA)
│   ├── manifest.json          # sizes, SHA-256, row counts
│   ├── README.md              # aggregate catalogue and provenance
│   └── QA.md                  # 32 integrity checks + sample comparison
├── scripts/
│   ├── prepare_data.sql       # parse → clean → sample → aggregate
│   ├── verify_data.sql        # integrity and representativeness checks
│   ├── make_manifest.mjs      # SHA-256 / size / row-count manifests
│   └── README.md              # usage, design decisions, runtime notes
├── data_local/                # gitignored row-level outputs (not published)
├── DISCLAIMER.txt             # official dataset disclaimer (verbatim)
├── *.pdf                      # official case study, rules and judging criteria
├── .gitignore
├── .gitattributes
└── README.md
```

**Not committed during the competition:** the 4.95 GB source CSV, `data_local/`
(row-level outputs), and `cs_findings.md` (private working notes).

---

## Quick start

1. Put the official file in the repository root:
   `swift_umdac_datathon_2026_fictitious_payment_data.csv`.
2. Install DuckDB v1.5.x and Node.js 20+ (Node only for manifests).
3. From the repository root:

```bash
duckdb -f scripts/prepare_data.sql
duckdb -f scripts/verify_data.sql
node scripts/make_manifest.mjs
```

Expected summary from the first step:

```
full_rows 10,000,000 | sample_rows 498,645 | accounts 391,132
sampled_accounts 19,670 | anomalies 7,006
```

Expected QA result: **32 / 32 checks PASS**.

---

## What the pipeline produces

### Public bundle (`data/`)

- **19 aggregate tables** computed from all 10M rows: amount distributions,
  currency mix, routing, path continuity, FX, duration, time-of-day/day,
  bank and intermediary concentration, account activity, and anomaly breakdowns.
- **`qa_checks.csv`** — 32 exact/range checks.
- **`qa_sample_vs_full.csv`** — 27 metrics comparing the local 5% sample with the
  full data.
- **`manifest.json`** — file sizes, SHA-256 hashes and row counts.

See [`data/README.md`](data/README.md) for every file and column, and
[`data/QA.md`](data/QA.md) for the QA report.

### Local bundle (`data_local/`, gitignored)

| File | Rows | Purpose |
|---|---:|---|
| `swift_umdac_2026_clean_full.parquet` | 10,000,000 | Cleaned full dataset (20 source + 7 derived columns) |
| `swift_umdac_2026_clean_sample_5pct.csv.gz` | 498,645 | Raw-like 5% account sample, exact 20 source columns |
| `swift_umdac_2026_clean_sample_5pct.parquet` | 498,645 | Typed sample with derived columns |
| `swift_umdac_2026_anomalies.csv.gz` | 7,006 | All rows with a non-blank anomaly label |
| `preview_100.csv` | 100 | Small mixed preview |

The sample selects **5% of ordering accounts within each ordering country** and
keeps all payments per selected account: 19,670 accounts, 498,645 rows (4.9864%),
all 228 ordering countries. Aggregates are always computed from the full data,
not the sample.

---

## Source schema

The source CSV has one header record, then records whose first field is the
disclaimer. The pipeline reads with `header=false, skip=1` and discards that
leading field. The 20 payment fields are:

| # | Column | Meaning |
|---:|---|---|
| 1 | `EPOCH_SECONDS` | Payment timestamp, Unix seconds (UTC) |
| 2 | `CURRENCY_CODE` | ISO 4217 currency code |
| 3 | `USD_AMOUNT` | Payment amount in USD |
| 4 | `ORDERING_BIC8` | 8-character ordering institution BIC |
| 5 | `BENEFICIARY_BIC8` | 8-character beneficiary institution BIC |
| 6 | `BIC8_PATH` | Semicolon-separated chain; `...` marks a hidden hop |
| 7 | `TOTAL_DURATION` | Total processing duration, seconds |
| 8 | `CONTINUITY_QFLAG` | `Y` / `N` completeness flag |
| 9 | `CURRENCY_CONVERSION_COUNT` | Number of FX conversions in the chain |
| 10 | `PARTICIPANT_COUNT` | Number of institutions in the chain |
| 11 | `TRANSACTION_TYPE_CODE` | `Direct`, `Indirect` or `Covered` |
| 12 | `ORDERING_ACCOUNT` | Ordering account identifier (may be blank) |
| 13 | `BENEFICIARY_ACCOUNT` | Beneficiary account identifier |
| 14 | `ORDERING_DETAILS` | Free-text ordering party details |
| 15 | `ORDERING_TOKENS` | Redacted token labels for ordering details |
| 16 | `BENEFICIARY_DETAILS` | Free-text beneficiary details |
| 17 | `BENEFICIARY_TOKENS` | Redacted token labels for beneficiary details |
| 18 | `ORDERING_TYPE` | `B` (business) or `C` (consumer) |
| 19 | `BENEFICIARY_TYPE` | `B` (business) or `C` (consumer) |
| 20 | `ANOMALY_TYPE` | Anomaly label, or blank |

### Derived columns (full and sample Parquet)

| Column | Definition |
|---|---|
| `EVENT_TS_UTC` | `EPOCH_SECONDS` as a UTC timestamp |
| `ORDERING_COUNTRY` | BIC characters 5–6 |
| `BENEFICIARY_COUNTRY` | BIC characters 5–6 |
| `PATH_START_MATCH` | First path token equals `ORDERING_BIC8` |
| `PATH_END_MATCH` | Last path token equals `BENEFICIARY_BIC8` |
| `PATH_GAP_COUNT` | Number of `...` tokens in `BIC8_PATH` |
| `IS_LOCAL_CURRENCY` | Currency equals the ordering country's modal currency |

No rows are removed: duplicate payments, extreme amounts and path-endpoint
substitutions are intentional generator behaviour and are preserved.

---

## Data quality

- **32 / 32 integrity checks pass**, including row count, epoch bounds, BIC and
  currency lengths, IBAN country/bank consistency, FX bounds, path continuity and
  anomaly vocabulary.
- **Sample representativeness:** routing mix, path-gap rates, endpoint match
  rates, local-currency share and anomaly rate are all within about one percentage
  point of the full data. Medians move by up to ~8% because of heavy-tailed sender
  activity in a 5% account sample — one reason the committed aggregates use the
  full data.

Full report: [`data/QA.md`](data/QA.md).

---

## Requirements and runtime

| Component | Version |
|---|---|
| DuckDB | v1.5.x (tested v1.5.6) |
| Node.js | 20+ (tested v26.13.1) |

Approximate runtime on an 8-core laptop: 3–8 minutes for `prepare_data.sql`,
1–3 minutes for `verify_data.sql`, under 1 minute for the manifest. A full rebuild
uses about 3 GB of disk.

---

## Licence and disclaimer

The dataset is fictitious and is © Swift 2026. See [`DISCLAIMER.txt`](DISCLAIMER.txt).
It must not be redistributed except as permitted by the UMDAC Datathon 2026 Rules
and Regulations. The code in this repository is provided for the competition; it
carries no separate licence or warranty.
