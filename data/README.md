# Aggregate data

This folder contains **full-data aggregates only** — small summary tables computed
from all 10,000,000 payment rows. No row-level records are published here.

The source dataset is fictitious and is © Swift 2026. See
[`../DISCLAIMER.txt`](../DISCLAIMER.txt). Do not redistribute the source data or
row-level extracts except as permitted by the UMDAC Datathon 2026 rules.

---

## 1. Provenance

| Item | Value |
|---|---|
| Source file | `swift_umdac_datathon_2026_fictitious_payment_data.csv` |
| Source size | 4.95 GB (not committed) |
| Rows | 10,000,000 payment records |
| Fields | 21: disclaimer + 20 payment fields |
| Time span | 2023-04-30 11:22:26 UTC → 2023-07-31 09:56:01 UTC |
| Currencies | 155 |
| Ordering countries | 228 |
| Beneficiary countries | 229 |
| Ordering BICs | 8,392 |
| Beneficiary BICs | 8,266 |
| Ordering accounts | 391,132 (non-empty) |
| Beneficiary accounts | 2,590,877 |
| Anomaly rows | 7,006 (0.0701%) |

All aggregates in this folder are built by [`../scripts/prepare_data.sql`](../scripts/prepare_data.sql)
and checked by [`../scripts/verify_data.sql`](../scripts/verify_data.sql) using
**DuckDB v1.5.6**.

### Parsing note

The source CSV is malformed by design: record 1 is a header, and the disclaimer
is the first field of record 2 (the first payment). The pipeline reads with
`header=false, skip=1` and discards the leading `column00` disclaimer field. A
naive parser will either mis-assign columns or corrupt the first payment.

### Derived columns

| Column | Definition |
|---|---|
| `ORDERING_COUNTRY` / `BENEFICIARY_COUNTRY` | Characters 5–6 of the BIC8 (ISO 3166-1 alpha-2) |
| `IS_LOCAL_CURRENCY` | `CURRENCY_CODE` equals the **modal** currency of the ordering country |
| `PATH_GAP_COUNT` | Number of `...` tokens in `BIC8_PATH` |
| `PATH_START_MATCH` / `PATH_END_MATCH` | First / last path token equals the ordering / beneficiary BIC |
| `EVENT_TS_UTC` | `make_timestamp(EPOCH_SECONDS * 1e6)` |

No rows were removed or repaired. Duplicate payments, extreme amounts and
path-endpoint substitutions are intentional properties of the generator and are
retained.

---

## 2. Sample design (local bundle only)

A compact row-level sample exists under `data_local/` for local development but is
**not committed** during the competition. Its design is recorded here for
reproducibility:

- Population: 391,132 non-empty ordering accounts.
- Select **5% of accounts within each ordering country** (`ceil(0.05 × n)`), using
  `row_number() OVER (PARTITION BY ordering_country ORDER BY hash(account))`.
- Keep **all payments** of each selected account.
- Result: **19,670 accounts**, **498,645 rows (4.9864%)**, **all 228 ordering
  countries**.

This preserves account-level behaviour (heavy-tailed activity, duplicate groups,
one BIC per account) while keeping every country represented. See
[`QA.md`](QA.md) for the sample-vs-full comparison.

---

## 3. File catalogue

Rows exclude the CSV header. All files are UTF-8 CSV with a header.

| File | Rows | Grain | Key columns |
|---|---:|---|---|
| `agg_amount_overall.csv` | 1 | whole dataset | `n_payments`, quantiles `p01`–`p99`, `mean_usd`, `max_usd` |
| `agg_amount_party_pair.csv` | 4 | ordering type × beneficiary type | `party_pair` (`B-B`, `B-C`, `C-B`, `C-C`), quantiles, `mean_usd`, `max_usd` |
| `agg_amount_country.csv` | 228 | ordering country | `ordering_country`, quantiles, `mean_usd`, `max_usd`, `share_pct` |
| `agg_currency_country.csv` | 13,804 | ordering country × currency | `is_local_currency` (0/1), `n_payments`, `share_pct` |
| `agg_currency_foreign_mix.csv` | 155 | foreign currency | `currency_code`, `n_payments`, `share_pct` |
| `agg_routing.csv` | 32 | transaction type × participant count | `transaction_type`, `participant_count`, `n_payments`, `share_pct` |
| `agg_path_continuity.csv` | 19 | participant count | observed vs predicted gap rate, endpoint match rates |
| `agg_fx_pooled.csv` | 90 | participant count × FX count | `fx_count`, `n_payments`, `share_pct` |
| `agg_fx_country.csv` | 10,488 | country × local flag × participant count × FX count | `is_local_currency` (true/false), `share_pct` |
| `agg_duration.csv` | 90 | participant count × FX count | `median_seconds`, `p90_seconds`, `p99_seconds`, `pct_over_1h` |
| `agg_time_hour.csv` | 5,308 | ordering country × UTC hour | `hour_utc`, `n_payments`, `median_usd` |
| `agg_time_daily.csv` | 93 | UTC date | `weekday`, `n_payments`, `median_usd`, `total_usd` |
| `agg_local_currency.csv` | 228 | ordering country | `local_currency`, `n_local_currency`, `local_share_pct` |
| `agg_anomaly_type.csv` | 6 | anomaly type | `share_of_all_pct`, `ordering_b_pct`, `beneficiary_c_pct`, `median_usd` |
| `agg_anomaly_country.csv` | 229 | beneficiary country | `n_anomalies`, `anomaly_rate_pct`, `top_anomaly_type` |
| `agg_anomaly_party_pair.csv` | 20 | anomaly type × party pair | `n_payments` |
| `agg_bank_country.csv` | 229 | bank country | distinct ordering / beneficiary / path BICs, path mentions |
| `agg_intermediary_concentration.csv` | 7,506 | intermediary BIC | `rank`, `mentions`, `share_pct`, `cumulative_share_pct` |
| `agg_account_activity.csv` | 12 | ordering type × activity bucket | `n_accounts`, `n_payments`, payment/account share |
| `qa_checks.csv` | 32 | one integrity check per row | `check_name`, `value`, `expected`, `status` |
| `qa_sample_vs_full.csv` | 27 | one metric per row | `full_value`, `sample_value`, `diff_sample_minus_full`, `ratio_sample_to_full` |

### Column conventions

- Amounts are **USD**, rounded to 2 decimals.
- Percentages are percentage points (`12.34` means 12.34%), usually 4 decimals;
  `share_of_all_pct` in `agg_anomaly_type.csv` uses 6 decimals.
- Country codes are ISO 3166-1 alpha-2, derived from the BIC.
- `is_local_currency` is `0/1` in `agg_currency_country.csv` but `true/false`
  in `agg_fx_country.csv`, matching the expression used to build each table.
- `ANOMALY_TYPE` is blank in the source for non-anomaly rows; anomaly aggregates
  only include non-blank rows.

---

## 4. Reproducing the aggregates

From the repository root, with the official CSV placed at the path above:

```bash
duckdb -f scripts/prepare_data.sql   # writes data/aggregates/*.csv
duckdb -f scripts/verify_data.sql    # writes qa_checks.csv and qa_sample_vs_full.csv
```

`prepare_data.sql` also writes row-level outputs to `data_local/` (gitignored):
the full cleaned Parquet, the 5% sample (CSV.gz + Parquet), all anomaly rows and a
100-row preview. Expect several minutes and ~3 GB of disk for a full rebuild.

See [`../scripts/README.md`](../scripts/README.md) for runtime notes and the local
file manifest.
