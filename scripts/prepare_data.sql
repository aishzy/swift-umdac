-- ============================================================================
-- UMDAC Datathon 2026 / Swift -- clean, sample and aggregate the payment data
-- ============================================================================
-- Run from the repository root:
--   duckdb -f scripts/prepare_data.sql
--
-- Prerequisite: place the official CSV in the repo root with its original name
--   swift_umdac_datathon_2026_fictitious_payment_data.csv
--
-- Creates:
--   data_local/swift_umdac_2026_clean_full.parquet
--   data_local/swift_umdac_2026_clean_sample_5pct.parquet
--   data_local/swift_umdac_2026_clean_sample_5pct.csv.gz
--   data_local/swift_umdac_2026_anomalies.csv.gz
--   data_local/preview_100.csv
--   data/aggregates/*.csv
--
-- Notes
--   * The source CSV has a real header row (record 1). The disclaimer is the
--     DISCLAIMER field of record 2 and is appended to the first payment. Reading
--     with header=false, skip=1 gives column00 (disclaimer) + the 20 real fields.
--   * No rows are removed or repaired. Duplicate payments, fat-finger amounts
--     and the 5% path-endpoint substitutions are intentional generator rules.
--   * The sample is 5% of ORDERING ACCOUNTS, stratified by ordering country,
--     keeping every payment of each selected account. This preserves
--     account-level behaviour and keeps all 228 ordering countries.
-- ============================================================================

SET threads = 8;
SET preserve_insertion_order = false;
SET temp_directory = 'data_local/duckdb_tmp';

-- ----------------------------------------------------------------------------
-- 1. Raw view
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW raw AS
SELECT *
FROM read_csv(
    'swift_umdac_datathon_2026_fictitious_payment_data.csv',
    header = false,
    skip = 1,
    all_varchar = true,
    quote = '"',
    escape = '"',
    delim = ','
);

-- ----------------------------------------------------------------------------
-- 2. Local currency per ordering country (mode of CURRENCY_CODE)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE local_ccy AS
WITH counts AS (
    SELECT
        substr(trim(column04), 5, 2) AS country,
        trim(column02)               AS currency,
        count(*)                     AS n
    FROM raw
    WHERE length(trim(column04)) = 8
    GROUP BY 1, 2
),
ranked AS (
    SELECT
        country,
        currency,
        n,
        row_number() OVER (PARTITION BY country ORDER BY n DESC, currency) AS rn
    FROM counts
)
SELECT country, currency AS local_currency, n AS n_local_currency
FROM ranked
WHERE rn = 1;

-- ----------------------------------------------------------------------------
-- 3. Full cleaned parquet (20 original columns + documented derived columns)
-- ----------------------------------------------------------------------------
COPY (
    SELECT
        CAST(trim(r.column01) AS BIGINT)               AS EPOCH_SECONDS,
        trim(r.column02)                               AS CURRENCY_CODE,
        CAST(trim(r.column03) AS DECIMAL(18, 2))       AS USD_AMOUNT,
        trim(r.column04)                               AS ORDERING_BIC8,
        trim(r.column05)                               AS BENEFICIARY_BIC8,
        trim(r.column06)                               AS BIC8_PATH,
        CAST(trim(r.column07) AS BIGINT)               AS TOTAL_DURATION,
        trim(r.column08)                               AS CONTINUITY_QFLAG,
        CAST(trim(r.column09) AS SMALLINT)             AS CURRENCY_CONVERSION_COUNT,
        CAST(trim(r.column10) AS SMALLINT)             AS PARTICIPANT_COUNT,
        trim(r.column11)                               AS TRANSACTION_TYPE_CODE,
        r.column12                                     AS ORDERING_ACCOUNT,
        r.column13                                     AS BENEFICIARY_ACCOUNT,
        r.column14                                     AS ORDERING_DETAILS,
        r.column15                                     AS ORDERING_TOKENS,
        r.column16                                     AS BENEFICIARY_DETAILS,
        r.column17                                     AS BENEFICIARY_TOKENS,
        trim(r.column18)                               AS ORDERING_TYPE,
        trim(r.column19)                               AS BENEFICIARY_TYPE,
        NULLIF(trim(r.column20), '')                   AS ANOMALY_TYPE,

        -- Derived convenience columns (not present in the source schema)
        make_timestamp(CAST(trim(r.column01) AS BIGINT) * 1000000) AS EVENT_TS_UTC,
        substr(trim(r.column04), 5, 2)                 AS ORDERING_COUNTRY,
        substr(trim(r.column05), 5, 2)                 AS BENEFICIARY_COUNTRY,
        list_extract(str_split(trim(r.column06), ';'), 1) = trim(r.column04) AS PATH_START_MATCH,
        list_last(str_split(trim(r.column06), ';')) = trim(r.column05)       AS PATH_END_MATCH,
        CAST(
            (length(trim(r.column06)) - length(replace(trim(r.column06), '...', ''))) / 3
            AS SMALLINT
        )                                              AS PATH_GAP_COUNT,
        CASE
            WHEN trim(r.column02) = l.local_currency THEN TRUE ELSE FALSE
        END                                            AS IS_LOCAL_CURRENCY
    FROM raw r
    LEFT JOIN local_ccy l
      ON substr(trim(r.column04), 5, 2) = l.country
)
TO 'data_local/swift_umdac_2026_clean_full.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);

CREATE OR REPLACE VIEW payments AS
SELECT * FROM read_parquet('data_local/swift_umdac_2026_clean_full.parquet');

-- ----------------------------------------------------------------------------
-- 4. Account-level table for stratified sampling
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE accounts AS
SELECT
    ORDERING_ACCOUNT,
    count(*)                          AS n_payments,
    count(DISTINCT ORDERING_BIC8)     AS n_bics,
    count(DISTINCT ORDERING_TYPE)     AS n_types,
    min(ORDERING_BIC8)                AS ordering_bic8,
    min(ORDERING_TYPE)                AS ordering_type,
    min(ORDERING_COUNTRY)             AS ordering_country
FROM payments
WHERE ORDERING_ACCOUNT IS NOT NULL
  AND length(trim(ORDERING_ACCOUNT)) > 0
GROUP BY ORDERING_ACCOUNT;

CREATE OR REPLACE TABLE account_strata AS
SELECT
    *,
    CASE
        WHEN n_payments = 1    THEN '1'
        WHEN n_payments = 2    THEN '2'
        WHEN n_payments <= 5   THEN '3-5'
        WHEN n_payments <= 20  THEN '6-20'
        WHEN n_payments <= 100 THEN '21-100'
        ELSE '101+'
    END AS activity_bucket
FROM accounts;

-- 5% of ordering accounts within each ordering country. ceil() guarantees that
-- every country with at least one account contributes at least one account, so
-- all 228 ordering countries remain represented. Within a country, selection is
-- a uniform hash sample, which preserves the party-type and activity mix in
-- expectation; keeping all payments of each selected account preserves
-- account-level behaviour (heavy tails, duplicate groups, one BIC per account).
CREATE OR REPLACE TABLE sampled_accounts AS
WITH ranked AS (
    SELECT
        ORDERING_ACCOUNT,
        row_number() OVER (
            PARTITION BY ordering_country
            ORDER BY hash(ORDERING_ACCOUNT)
        ) AS rn,
        count(*) OVER (
            PARTITION BY ordering_country
        ) AS n
    FROM account_strata
)
SELECT ORDERING_ACCOUNT
FROM ranked
WHERE rn <= CAST(ceil(0.05 * n) AS BIGINT);

CREATE OR REPLACE TABLE sample_rows AS
SELECT f.*
FROM payments f
SEMI JOIN sampled_accounts s
  ON f.ORDERING_ACCOUNT = s.ORDERING_ACCOUNT;

-- ----------------------------------------------------------------------------
-- 5. Sample outputs
-- ----------------------------------------------------------------------------
COPY (
    SELECT
        EPOCH_SECONDS, CURRENCY_CODE, USD_AMOUNT,
        ORDERING_BIC8, BENEFICIARY_BIC8, BIC8_PATH, TOTAL_DURATION,
        CONTINUITY_QFLAG, CURRENCY_CONVERSION_COUNT, PARTICIPANT_COUNT,
        TRANSACTION_TYPE_CODE, ORDERING_ACCOUNT, BENEFICIARY_ACCOUNT,
        ORDERING_DETAILS, ORDERING_TOKENS, BENEFICIARY_DETAILS,
        BENEFICIARY_TOKENS, ORDERING_TYPE, BENEFICIARY_TYPE, ANOMALY_TYPE
    FROM sample_rows
    ORDER BY EPOCH_SECONDS, ORDERING_ACCOUNT
)
TO 'data_local/swift_umdac_2026_clean_sample_5pct.csv.gz'
(FORMAT CSV, HEADER, COMPRESSION GZIP);

COPY (
    SELECT *
    FROM sample_rows
    ORDER BY EPOCH_SECONDS, ORDERING_ACCOUNT
)
TO 'data_local/swift_umdac_2026_clean_sample_5pct.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);

COPY (
    SELECT
        EPOCH_SECONDS, CURRENCY_CODE, USD_AMOUNT,
        ORDERING_BIC8, BENEFICIARY_BIC8, BIC8_PATH, TOTAL_DURATION,
        CONTINUITY_QFLAG, CURRENCY_CONVERSION_COUNT, PARTICIPANT_COUNT,
        TRANSACTION_TYPE_CODE, ORDERING_ACCOUNT, BENEFICIARY_ACCOUNT,
        ORDERING_DETAILS, ORDERING_TOKENS, BENEFICIARY_DETAILS,
        BENEFICIARY_TOKENS, ORDERING_TYPE, BENEFICIARY_TYPE, ANOMALY_TYPE
    FROM payments
    WHERE ANOMALY_TYPE IS NOT NULL
    ORDER BY EPOCH_SECONDS, ORDERING_ACCOUNT
)
TO 'data_local/swift_umdac_2026_anomalies.csv.gz'
(FORMAT CSV, HEADER, COMPRESSION GZIP);

COPY (
    SELECT
        EPOCH_SECONDS, CURRENCY_CODE, USD_AMOUNT,
        ORDERING_BIC8, BENEFICIARY_BIC8, BIC8_PATH, TOTAL_DURATION,
        CONTINUITY_QFLAG, CURRENCY_CONVERSION_COUNT, PARTICIPANT_COUNT,
        TRANSACTION_TYPE_CODE, ORDERING_ACCOUNT, BENEFICIARY_ACCOUNT,
        ORDERING_DETAILS, ORDERING_TOKENS, BENEFICIARY_DETAILS,
        BENEFICIARY_TOKENS, ORDERING_TYPE, BENEFICIARY_TYPE, ANOMALY_TYPE
    FROM sample_rows
    ORDER BY hash(ORDERING_ACCOUNT || '|' || EPOCH_SECONDS)
    LIMIT 100
)
TO 'data_local/preview_100.csv'
(FORMAT CSV, HEADER);

-- ----------------------------------------------------------------------------
-- 6. Aggregates (computed from the full 10M rows, not the sample)
-- ----------------------------------------------------------------------------
COPY (
    SELECT
        ORDERING_TYPE || '-' || BENEFICIARY_TYPE AS party_pair,
        count(*)                                  AS n_payments,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 4) AS share_pct,
        round(median(USD_AMOUNT::DOUBLE), 2)      AS median_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.10), 2) AS p10_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.25), 2) AS p25_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.75), 2) AS p75_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.90), 2) AS p90_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.99), 2) AS p99_usd,
        round(avg(USD_AMOUNT::DOUBLE), 2)         AS mean_usd,
        round(max(USD_AMOUNT::DOUBLE), 2)         AS max_usd
    FROM payments
    GROUP BY 1
    ORDER BY 1
)
TO 'data/aggregates/agg_amount_party_pair.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        count(*)                                  AS n_payments,
        round(median(USD_AMOUNT::DOUBLE), 2)      AS median_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.01), 2) AS p01_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.25), 2) AS p25_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.75), 2) AS p75_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.90), 2) AS p90_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.99), 2) AS p99_usd,
        round(avg(USD_AMOUNT::DOUBLE), 2)         AS mean_usd,
        round(max(USD_AMOUNT::DOUBLE), 2)         AS max_usd
    FROM payments
)
TO 'data/aggregates/agg_amount_overall.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        ORDERING_COUNTRY                          AS ordering_country,
        count(*)                                  AS n_payments,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 4) AS share_pct,
        round(median(USD_AMOUNT::DOUBLE), 2)      AS median_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.25), 2) AS p25_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.75), 2) AS p75_usd,
        round(quantile_cont(USD_AMOUNT::DOUBLE, 0.90), 2) AS p90_usd,
        round(avg(USD_AMOUNT::DOUBLE), 2)         AS mean_usd,
        round(max(USD_AMOUNT::DOUBLE), 2)         AS max_usd
    FROM payments
    GROUP BY 1
    ORDER BY median_usd DESC NULLS LAST
)
TO 'data/aggregates/agg_amount_country.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        ORDERING_COUNTRY                          AS ordering_country,
        CURRENCY_CODE                             AS currency_code,
        max(CASE WHEN IS_LOCAL_CURRENCY THEN 1 ELSE 0 END) AS is_local_currency,
        count(*)                                  AS n_payments,
        round(count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY ORDERING_COUNTRY), 4) AS share_pct
    FROM payments
    GROUP BY 1, 2
    ORDER BY 1, n_payments DESC
)
TO 'data/aggregates/agg_currency_country.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        CURRENCY_CODE                             AS currency_code,
        count(*)                                  AS n_payments,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 4) AS share_pct
    FROM payments
    WHERE NOT IS_LOCAL_CURRENCY
    GROUP BY 1
    ORDER BY n_payments DESC
)
TO 'data/aggregates/agg_currency_foreign_mix.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        TRANSACTION_TYPE_CODE                     AS transaction_type,
        PARTICIPANT_COUNT                         AS participant_count,
        count(*)                                  AS n_payments,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 4) AS share_pct
    FROM payments
    GROUP BY 1, 2
    ORDER BY 1, 2
)
TO 'data/aggregates/agg_routing.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        PARTICIPANT_COUNT                         AS participant_count,
        count(*)                                  AS n_payments,
        count(*) FILTER (WHERE PATH_GAP_COUNT > 0) AS n_with_gap,
        round(count(*) FILTER (WHERE PATH_GAP_COUNT > 0) * 100.0 / count(*), 4) AS gap_rate_pct_observed,
        round((1 - power(0.95, PARTICIPANT_COUNT - 1)) * 100.0, 4) AS gap_rate_pct_predicted,
        round(count(*) FILTER (WHERE PATH_START_MATCH) * 100.0 / count(*), 4) AS start_match_pct,
        round(count(*) FILTER (WHERE PATH_END_MATCH) * 100.0 / count(*), 4) AS end_match_pct,
        round(count(*) FILTER (WHERE NOT PATH_START_MATCH OR NOT PATH_END_MATCH) * 100.0 / count(*), 4) AS either_mismatch_pct
    FROM payments
    GROUP BY 1
    ORDER BY 1
)
TO 'data/aggregates/agg_path_continuity.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        PARTICIPANT_COUNT                         AS participant_count,
        CURRENCY_CONVERSION_COUNT                 AS fx_count,
        count(*)                                  AS n_payments,
        round(count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY PARTICIPANT_COUNT), 4) AS share_pct
    FROM payments
    GROUP BY 1, 2
    ORDER BY 1, 2
)
TO 'data/aggregates/agg_fx_pooled.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        ORDERING_COUNTRY                          AS ordering_country,
        IS_LOCAL_CURRENCY                         AS is_local_currency,
        PARTICIPANT_COUNT                         AS participant_count,
        CURRENCY_CONVERSION_COUNT                 AS fx_count,
        count(*)                                  AS n_payments,
        round(count(*) * 100.0 / sum(count(*)) OVER (
            PARTITION BY ORDERING_COUNTRY, IS_LOCAL_CURRENCY, PARTICIPANT_COUNT
        ), 4) AS share_pct
    FROM payments
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
)
TO 'data/aggregates/agg_fx_country.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        PARTICIPANT_COUNT                         AS participant_count,
        CURRENCY_CONVERSION_COUNT                 AS fx_count,
        count(*)                                  AS n_payments,
        round(median(TOTAL_DURATION::DOUBLE), 1)  AS median_seconds,
        round(quantile_cont(TOTAL_DURATION::DOUBLE, 0.90), 1) AS p90_seconds,
        round(quantile_cont(TOTAL_DURATION::DOUBLE, 0.99), 1) AS p99_seconds,
        round(avg(TOTAL_DURATION::DOUBLE), 1)     AS mean_seconds,
        round(count(*) FILTER (WHERE TOTAL_DURATION > 3600) * 100.0 / count(*), 2) AS pct_over_1h
    FROM payments
    GROUP BY 1, 2
    ORDER BY 1, 2
)
TO 'data/aggregates/agg_duration.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        ORDERING_COUNTRY                          AS ordering_country,
        hour(EVENT_TS_UTC)                        AS hour_utc,
        count(*)                                  AS n_payments,
        round(median(USD_AMOUNT::DOUBLE), 2)      AS median_usd
    FROM payments
    GROUP BY 1, 2
    ORDER BY 1, 2
)
TO 'data/aggregates/agg_time_hour.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        CAST(EVENT_TS_UTC AS DATE)                AS date_utc,
        dayname(EVENT_TS_UTC)                     AS weekday,
        count(*)                                  AS n_payments,
        round(median(USD_AMOUNT::DOUBLE), 2)      AS median_usd,
        round(sum(USD_AMOUNT::DOUBLE), 2)         AS total_usd
    FROM payments
    GROUP BY 1, 2
    ORDER BY 1
)
TO 'data/aggregates/agg_time_daily.csv' (FORMAT CSV, HEADER);

COPY (
    WITH totals AS (
        SELECT ORDERING_COUNTRY AS country, count(*) AS n_payments
        FROM payments
        GROUP BY 1
    )
    SELECT
        l.country                                 AS ordering_country,
        l.local_currency                          AS local_currency,
        l.n_local_currency                        AS n_local_currency,
        t.n_payments                              AS n_payments,
        round(l.n_local_currency * 100.0 / t.n_payments, 4) AS local_share_pct
    FROM local_ccy l
    JOIN totals t
      ON l.country = t.country
    ORDER BY 1
)
TO 'data/aggregates/agg_local_currency.csv' (FORMAT CSV, HEADER);

COPY (
    WITH totals AS (
        SELECT count(*) AS total FROM payments
    ),
    by_type AS (
        SELECT
            ANOMALY_TYPE                          AS anomaly_type,
            count(*)                              AS n_payments,
            round(count(*) FILTER (WHERE ORDERING_TYPE = 'B') * 100.0 / count(*), 2) AS ordering_b_pct,
            round(count(*) FILTER (WHERE BENEFICIARY_TYPE = 'C') * 100.0 / count(*), 2) AS beneficiary_c_pct,
            round(median(USD_AMOUNT::DOUBLE), 2)  AS median_usd
        FROM payments
        WHERE ANOMALY_TYPE IS NOT NULL
        GROUP BY 1
    )
    SELECT
        b.anomaly_type,
        b.n_payments,
        round(b.n_payments * 100.0 / t.total, 6) AS share_of_all_pct,
        b.ordering_b_pct,
        b.beneficiary_c_pct,
        b.median_usd
    FROM by_type b
    CROSS JOIN totals t
    ORDER BY b.n_payments DESC
)
TO 'data/aggregates/agg_anomaly_type.csv' (FORMAT CSV, HEADER);

COPY (
    WITH base AS (
        SELECT
            BENEFICIARY_COUNTRY                   AS beneficiary_country,
            count(*)                              AS n_payments,
            count(*) FILTER (WHERE ANOMALY_TYPE IS NOT NULL) AS n_anomalies
        FROM payments
        GROUP BY 1
    ),
    types AS (
        SELECT
            BENEFICIARY_COUNTRY                   AS beneficiary_country,
            ANOMALY_TYPE                          AS anomaly_type,
            count(*)                              AS n
        FROM payments
        WHERE ANOMALY_TYPE IS NOT NULL
        GROUP BY 1, 2
    ),
    ranked AS (
        SELECT *, row_number() OVER (
            PARTITION BY beneficiary_country
            ORDER BY n DESC, anomaly_type
        ) AS rn
        FROM types
    )
    SELECT
        b.beneficiary_country,
        b.n_payments,
        b.n_anomalies,
        round(b.n_anomalies * 100.0 / b.n_payments, 4) AS anomaly_rate_pct,
        r.anomaly_type                               AS top_anomaly_type,
        r.n                                          AS top_anomaly_count
    FROM base b
    LEFT JOIN ranked r
      ON b.beneficiary_country = r.beneficiary_country
     AND r.rn = 1
    ORDER BY anomaly_rate_pct DESC NULLS LAST
)
TO 'data/aggregates/agg_anomaly_country.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        ANOMALY_TYPE                              AS anomaly_type,
        ORDERING_TYPE || '-' || BENEFICIARY_TYPE  AS party_pair,
        count(*)                                  AS n_payments
    FROM payments
    WHERE ANOMALY_TYPE IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
)
TO 'data/aggregates/agg_anomaly_party_pair.csv' (FORMAT CSV, HEADER);

COPY (
    WITH endpoint_bics AS (
        SELECT ORDERING_BIC8 AS bic, 'ordering' AS role FROM payments
        UNION ALL
        SELECT BENEFICIARY_BIC8 AS bic, 'beneficiary' AS role FROM payments
    ),
    path_bics AS (
        SELECT unnest(str_split(BIC8_PATH, ';')) AS bic
        FROM payments
        WHERE BIC8_PATH IS NOT NULL
    ),
    all_bics AS (
        SELECT bic, role FROM endpoint_bics
        UNION ALL
        SELECT bic, 'path' AS role FROM path_bics
    )
    SELECT
        substr(bic, 5, 2)                         AS country,
        count(DISTINCT bic) FILTER (WHERE role = 'ordering')    AS n_ordering_bics,
        count(DISTINCT bic) FILTER (WHERE role = 'beneficiary') AS n_beneficiary_bics,
        count(*) FILTER (WHERE role = 'path')     AS n_path_mentions,
        count(DISTINCT bic)                       AS n_bics
    FROM all_bics
    WHERE bic <> '...'
      AND length(bic) = 8
    GROUP BY 1
    ORDER BY n_bics DESC
)
TO 'data/aggregates/agg_bank_country.csv' (FORMAT CSV, HEADER);

COPY (
    WITH mids_str AS (
        SELECT regexp_extract(BIC8_PATH, '^[^;]+;(.*);[^;]+$', 1) AS middle
        FROM payments
        WHERE BIC8_PATH LIKE '%;%;%'
    ),
    mids AS (
        SELECT unnest(str_split(middle, ';')) AS bic
        FROM mids_str
    ),
    counts AS (
        SELECT bic, count(*) AS mentions
        FROM mids
        WHERE bic <> '...'
          AND length(bic) = 8
        GROUP BY 1
    )
    SELECT
        row_number() OVER (ORDER BY mentions DESC, bic) AS rank,
        bic,
        substr(bic, 5, 2)                         AS country,
        mentions,
        round(mentions * 100.0 / sum(mentions) OVER (), 4) AS share_pct,
        round(
            sum(mentions) OVER (
                ORDER BY mentions DESC, bic
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) * 100.0 / sum(mentions) OVER (),
            4
        )                                         AS cumulative_share_pct
    FROM counts
    ORDER BY rank
)
TO 'data/aggregates/agg_intermediary_concentration.csv' (FORMAT CSV, HEADER);

COPY (
    SELECT
        ordering_type                             AS ordering_type,
        activity_bucket                           AS activity_bucket,
        count(*)                                  AS n_accounts,
        sum(n_payments)                           AS n_payments,
        round(sum(n_payments) * 100.0 / sum(sum(n_payments)) OVER (), 4) AS share_of_payments_pct,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 4) AS share_of_accounts_pct
    FROM account_strata
    GROUP BY 1, 2
    ORDER BY 1, 2
)
TO 'data/aggregates/agg_account_activity.csv' (FORMAT CSV, HEADER);

-- ----------------------------------------------------------------------------
-- 7. Run summary (printed to stdout)
-- ----------------------------------------------------------------------------
SELECT
    (SELECT count(*) FROM payments)   AS full_rows,
    (SELECT count(*) FROM sample_rows) AS sample_rows,
    (SELECT count(*) FROM accounts) AS accounts,
    (SELECT count(*) FROM sampled_accounts) AS sampled_accounts,
    (SELECT count(*) FROM payments WHERE ANOMALY_TYPE IS NOT NULL) AS anomalies;
