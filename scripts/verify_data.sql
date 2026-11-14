-- ============================================================================
-- UMDAC Datathon 2026 / Swift -- data integrity and representativeness checks
-- ============================================================================
-- Run after scripts/prepare_data.sql:
--   duckdb -f scripts/verify_data.sql
--
-- Outputs:
--   data/aggregates/qa_checks.csv
--   data/aggregates/qa_sample_vs_full.csv
-- ============================================================================

SET threads = 8;
SET preserve_insertion_order = false;
SET temp_directory = 'data_local/duckdb_tmp';

CREATE OR REPLACE VIEW payments AS
SELECT * FROM read_parquet('data_local/swift_umdac_2026_clean_full.parquet');

CREATE OR REPLACE VIEW sample_rows AS
SELECT * FROM read_parquet('data_local/swift_umdac_2026_clean_sample_5pct.parquet');

-- ----------------------------------------------------------------------------
-- 1. Hard integrity checks
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE qa_checks (
    check_name VARCHAR,
    value      VARCHAR,
    expected   VARCHAR,
    status     VARCHAR
);

INSERT INTO qa_checks SELECT
    'row_count',
    CAST(count(*) AS VARCHAR), '10000000',
    CASE WHEN count(*) = 10000000 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'min_epoch_seconds',
    CAST(min(EPOCH_SECONDS) AS VARCHAR), '1682853746',
    CASE WHEN min(EPOCH_SECONDS) = 1682853746 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'max_epoch_seconds',
    CAST(max(EPOCH_SECONDS) AS VARCHAR), '1690797361',
    CASE WHEN max(EPOCH_SECONDS) = 1690797361 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'first_payment_recovered',
    CAST(count(*) FILTER (WHERE EPOCH_SECONDS = 1685363344) AS VARCHAR), '>= 1',
    CASE WHEN count(*) FILTER (WHERE EPOCH_SECONDS = 1685363344) >= 1 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'null_epoch_rows',
    CAST(count(*) FILTER (WHERE EPOCH_SECONDS IS NULL) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE EPOCH_SECONDS IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'null_amount_rows',
    CAST(count(*) FILTER (WHERE USD_AMOUNT IS NULL) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE USD_AMOUNT IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'nonpositive_amount_rows',
    CAST(count(*) FILTER (WHERE USD_AMOUNT <= 0) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE USD_AMOUNT <= 0) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'max_usd_amount',
    CAST(max(USD_AMOUNT) AS VARCHAR), '15664132213.09',
    CASE WHEN max(USD_AMOUNT) = 15664132213.09 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'participants_below_2',
    CAST(count(*) FILTER (WHERE PARTICIPANT_COUNT < 2) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE PARTICIPANT_COUNT < 2) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'fx_count_above_participants_minus_1',
    CAST(count(*) FILTER (WHERE CURRENCY_CONVERSION_COUNT > PARTICIPANT_COUNT - 1) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE CURRENCY_CONVERSION_COUNT > PARTICIPANT_COUNT - 1) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'bad_ordering_bic_length',
    CAST(count(*) FILTER (WHERE length(ORDERING_BIC8) <> 8) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE length(ORDERING_BIC8) <> 8) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'bad_beneficiary_bic_length',
    CAST(count(*) FILTER (WHERE length(BENEFICIARY_BIC8) <> 8) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE length(BENEFICIARY_BIC8) <> 8) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'bad_currency_length',
    CAST(count(*) FILTER (WHERE length(CURRENCY_CODE) <> 3) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE length(CURRENCY_CODE) <> 3) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'invalid_continuity_flag',
    CAST(count(*) FILTER (WHERE CONTINUITY_QFLAG IS NULL OR CONTINUITY_QFLAG NOT IN ('Y', 'N')) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE CONTINUITY_QFLAG IS NULL OR CONTINUITY_QFLAG NOT IN ('Y', 'N')) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'invalid_transaction_type',
    CAST(count(*) FILTER (WHERE TRANSACTION_TYPE_CODE NOT IN ('Direct', 'Indirect', 'Covered')) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE TRANSACTION_TYPE_CODE NOT IN ('Direct', 'Indirect', 'Covered')) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'invalid_party_type',
    CAST(count(*) FILTER (WHERE ORDERING_TYPE NOT IN ('B', 'C') OR BENEFICIARY_TYPE NOT IN ('B', 'C')) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (WHERE ORDERING_TYPE NOT IN ('B', 'C') OR BENEFICIARY_TYPE NOT IN ('B', 'C')) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'invalid_anomaly_type',
    CAST(count(*) FILTER (
        WHERE ANOMALY_TYPE NOT IN (
            'ROMANCE_SCAM', 'FAT_FINGER_MISTAKE', 'CEO_SCAM',
            'DUPLICATE_PAYMENT', 'ACCOUNT_REPLACEMENT_SCAM', 'CUSTOMER_RECALL'
        )
    ) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (
        WHERE ANOMALY_TYPE NOT IN (
            'ROMANCE_SCAM', 'FAT_FINGER_MISTAKE', 'CEO_SCAM',
            'DUPLICATE_PAYMENT', 'ACCOUNT_REPLACEMENT_SCAM', 'CUSTOMER_RECALL'
        )
    ) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'distinct_currencies',
    CAST(count(DISTINCT CURRENCY_CODE) AS VARCHAR), '155',
    CASE WHEN count(DISTINCT CURRENCY_CODE) = 155 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'distinct_ordering_countries',
    CAST(count(DISTINCT ORDERING_COUNTRY) AS VARCHAR), '228',
    CASE WHEN count(DISTINCT ORDERING_COUNTRY) = 228 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'distinct_beneficiary_countries',
    CAST(count(DISTINCT BENEFICIARY_COUNTRY) AS VARCHAR), '229',
    CASE WHEN count(DISTINCT BENEFICIARY_COUNTRY) = 229 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'accounts_with_multiple_bics',
    CAST(count(*) AS VARCHAR), '0',
    CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT ORDERING_ACCOUNT
    FROM payments
    WHERE ORDERING_ACCOUNT IS NOT NULL AND length(trim(ORDERING_ACCOUNT)) > 0
    GROUP BY 1
    HAVING count(DISTINCT ORDERING_BIC8) > 1
) x;

INSERT INTO qa_checks SELECT
    'accounts_with_multiple_types',
    CAST(count(*) AS VARCHAR), '0',
    CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT ORDERING_ACCOUNT
    FROM payments
    WHERE ORDERING_ACCOUNT IS NOT NULL AND length(trim(ORDERING_ACCOUNT)) > 0
    GROUP BY 1
    HAVING count(DISTINCT ORDERING_TYPE) > 1
) x;

INSERT INTO qa_checks SELECT
    'anomaly_count',
    CAST(count(*) AS VARCHAR), '7006',
    CASE WHEN count(*) = 7006 THEN 'PASS' ELSE 'FAIL' END
FROM payments
WHERE ANOMALY_TYPE IS NOT NULL;

INSERT INTO qa_checks SELECT
    'distinct_anomaly_types',
    CAST(count(DISTINCT ANOMALY_TYPE) AS VARCHAR), '6',
    CASE WHEN count(DISTINCT ANOMALY_TYPE) = 6 THEN 'PASS' ELSE 'FAIL' END
FROM payments
WHERE ANOMALY_TYPE IS NOT NULL;

INSERT INTO qa_checks SELECT
    'iban_ordering_country_or_bank_mismatch',
    CAST(count(*) FILTER (
        WHERE length(ORDERING_ACCOUNT) = 28
          AND starts_with(ORDERING_ACCOUNT, '_FAKE_')
          AND (
              substr(ORDERING_ACCOUNT, 7, 2) <> substr(ORDERING_BIC8, 5, 2)
              OR substr(ORDERING_ACCOUNT, 11, 4) <> substr(ORDERING_BIC8, 1, 4)
          )
    ) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (
        WHERE length(ORDERING_ACCOUNT) = 28
          AND starts_with(ORDERING_ACCOUNT, '_FAKE_')
          AND (
              substr(ORDERING_ACCOUNT, 7, 2) <> substr(ORDERING_BIC8, 5, 2)
              OR substr(ORDERING_ACCOUNT, 11, 4) <> substr(ORDERING_BIC8, 1, 4)
          )
    ) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'iban_beneficiary_country_or_bank_mismatch',
    CAST(count(*) FILTER (
        WHERE length(BENEFICIARY_ACCOUNT) = 28
          AND starts_with(BENEFICIARY_ACCOUNT, '_FAKE_')
          AND (
              substr(BENEFICIARY_ACCOUNT, 7, 2) <> substr(BENEFICIARY_BIC8, 5, 2)
              OR substr(BENEFICIARY_ACCOUNT, 11, 4) <> substr(BENEFICIARY_BIC8, 1, 4)
          )
    ) AS VARCHAR), '0',
    CASE WHEN count(*) FILTER (
        WHERE length(BENEFICIARY_ACCOUNT) = 28
          AND starts_with(BENEFICIARY_ACCOUNT, '_FAKE_')
          AND (
              substr(BENEFICIARY_ACCOUNT, 7, 2) <> substr(BENEFICIARY_BIC8, 5, 2)
              OR substr(BENEFICIARY_ACCOUNT, 11, 4) <> substr(BENEFICIARY_BIC8, 1, 4)
          )
    ) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks SELECT
    'path_with_ellipsis_at_endpoint',
    CAST(count(*) AS VARCHAR), '0',
    CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM payments
WHERE list_extract(str_split(BIC8_PATH, ';'), 1) = '...'
   OR list_last(str_split(BIC8_PATH, ';')) = '...';

INSERT INTO qa_checks
SELECT
    'path_gap_formula_max_deviation_pp',
    -- Tolerance of 0.2 pp: the observed per-participant gap rates match
    -- 1-(1-0.95)^(n-1) within <0.1 pp, which is the expected sampling noise.
    CAST(round(max(abs(obs_pct - pred_pct)), 4) AS VARCHAR), '< 0.2',
    CASE WHEN max(abs(obs_pct - pred_pct)) < 0.2 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT
        PARTICIPANT_COUNT,
        count(*) FILTER (WHERE PATH_GAP_COUNT > 0) * 100.0 / count(*) AS obs_pct,
        (1 - power(0.95, PARTICIPANT_COUNT - 1)) * 100.0 AS pred_pct
    FROM payments
    WHERE PARTICIPANT_COUNT BETWEEN 2 AND 6
    GROUP BY 1
) x;

INSERT INTO qa_checks
SELECT
    'endpoint_start_match_pct',
    CAST(round(count(*) FILTER (WHERE PATH_START_MATCH) * 100.0 / count(*), 4) AS VARCHAR),
    '94.5-95.5',
    CASE WHEN count(*) FILTER (WHERE PATH_START_MATCH) * 100.0 / count(*) BETWEEN 94.5 AND 95.5 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks
SELECT
    'endpoint_end_match_pct',
    CAST(round(count(*) FILTER (WHERE PATH_END_MATCH) * 100.0 / count(*), 4) AS VARCHAR),
    '94.5-95.5',
    CASE WHEN count(*) FILTER (WHERE PATH_END_MATCH) * 100.0 / count(*) BETWEEN 94.5 AND 95.5 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks
SELECT
    'local_currency_share_pct',
    CAST(round(count(*) FILTER (WHERE IS_LOCAL_CURRENCY) * 100.0 / count(*), 4) AS VARCHAR),
    '89.0-90.0',
    CASE WHEN count(*) FILTER (WHERE IS_LOCAL_CURRENCY) * 100.0 / count(*) BETWEEN 89.0 AND 90.0 THEN 'PASS' ELSE 'FAIL' END
FROM payments;

INSERT INTO qa_checks
SELECT
    'sample_fraction_pct',
    CAST(round((SELECT count(*) FROM sample_rows) * 100.0 / (SELECT count(*) FROM payments), 4) AS VARCHAR),
    '4.5-5.5',
    CASE WHEN (SELECT count(*) FROM sample_rows) * 100.0 / (SELECT count(*) FROM payments) BETWEEN 4.5 AND 5.5 THEN 'PASS' ELSE 'FAIL' END;

-- ----------------------------------------------------------------------------
-- 2. Full vs sample representativeness
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE qa_sample_wide AS
WITH combined AS (
    SELECT
        'full' AS dataset,
        ORDERING_ACCOUNT, ORDERING_COUNTRY, BENEFICIARY_COUNTRY, CURRENCY_CODE,
        IS_LOCAL_CURRENCY, ORDERING_TYPE, BENEFICIARY_TYPE,
        TRANSACTION_TYPE_CODE, USD_AMOUNT,
        PARTICIPANT_COUNT, PATH_GAP_COUNT, PATH_START_MATCH, PATH_END_MATCH,
        ANOMALY_TYPE, TOTAL_DURATION
    FROM payments
    UNION ALL
    SELECT
        'sample',
        ORDERING_ACCOUNT, ORDERING_COUNTRY, BENEFICIARY_COUNTRY, CURRENCY_CODE,
        IS_LOCAL_CURRENCY, ORDERING_TYPE, BENEFICIARY_TYPE,
        TRANSACTION_TYPE_CODE, USD_AMOUNT,
        PARTICIPANT_COUNT, PATH_GAP_COUNT, PATH_START_MATCH, PATH_END_MATCH,
        ANOMALY_TYPE, TOTAL_DURATION
    FROM sample_rows
)
SELECT
    dataset,
    CAST(count(*) AS DOUBLE) AS rows,
    CAST(count(DISTINCT ORDERING_ACCOUNT) AS DOUBLE) AS ordering_accounts,
    CAST(count(DISTINCT ORDERING_COUNTRY) AS DOUBLE) AS ordering_countries,
    CAST(count(DISTINCT BENEFICIARY_COUNTRY) AS DOUBLE) AS beneficiary_countries,
    CAST(count(DISTINCT CURRENCY_CODE) AS DOUBLE) AS currencies,
    round(count(*) FILTER (WHERE IS_LOCAL_CURRENCY) * 100.0 / count(*), 4) AS local_currency_pct,
    round(count(*) FILTER (WHERE ORDERING_TYPE = 'B' AND BENEFICIARY_TYPE = 'B') * 100.0 / count(*), 4) AS bb_pct,
    round(count(*) FILTER (WHERE ORDERING_TYPE = 'B' AND BENEFICIARY_TYPE = 'C') * 100.0 / count(*), 4) AS bc_pct,
    round(count(*) FILTER (WHERE ORDERING_TYPE = 'C' AND BENEFICIARY_TYPE = 'B') * 100.0 / count(*), 4) AS cb_pct,
    round(count(*) FILTER (WHERE ORDERING_TYPE = 'C' AND BENEFICIARY_TYPE = 'C') * 100.0 / count(*), 4) AS cc_pct,
    round(count(*) FILTER (WHERE TRANSACTION_TYPE_CODE = 'Direct') * 100.0 / count(*), 4) AS direct_pct,
    round(count(*) FILTER (WHERE TRANSACTION_TYPE_CODE = 'Indirect') * 100.0 / count(*), 4) AS indirect_pct,
    round(count(*) FILTER (WHERE TRANSACTION_TYPE_CODE = 'Covered') * 100.0 / count(*), 4) AS covered_pct,
    round(median(USD_AMOUNT::DOUBLE), 2) AS median_usd,
    round(median(CASE WHEN ORDERING_TYPE = 'B' AND BENEFICIARY_TYPE = 'B' THEN USD_AMOUNT::DOUBLE END), 2) AS median_bb_usd,
    round(median(CASE WHEN ORDERING_TYPE = 'B' AND BENEFICIARY_TYPE = 'C' THEN USD_AMOUNT::DOUBLE END), 2) AS median_bc_usd,
    round(median(CASE WHEN ORDERING_TYPE = 'C' AND BENEFICIARY_TYPE = 'B' THEN USD_AMOUNT::DOUBLE END), 2) AS median_cb_usd,
    round(median(CASE WHEN ORDERING_TYPE = 'C' AND BENEFICIARY_TYPE = 'C' THEN USD_AMOUNT::DOUBLE END), 2) AS median_cc_usd,
    round(count(*) FILTER (WHERE PARTICIPANT_COUNT = 2 AND PATH_GAP_COUNT > 0) * 100.0
          / nullif(count(*) FILTER (WHERE PARTICIPANT_COUNT = 2), 0), 4) AS gap_p2_pct,
    round(count(*) FILTER (WHERE PARTICIPANT_COUNT = 3 AND PATH_GAP_COUNT > 0) * 100.0
          / nullif(count(*) FILTER (WHERE PARTICIPANT_COUNT = 3), 0), 4) AS gap_p3_pct,
    round(count(*) FILTER (WHERE PARTICIPANT_COUNT = 4 AND PATH_GAP_COUNT > 0) * 100.0
          / nullif(count(*) FILTER (WHERE PARTICIPANT_COUNT = 4), 0), 4) AS gap_p4_pct,
    round(count(*) FILTER (WHERE PARTICIPANT_COUNT = 5 AND PATH_GAP_COUNT > 0) * 100.0
          / nullif(count(*) FILTER (WHERE PARTICIPANT_COUNT = 5), 0), 4) AS gap_p5_pct,
    round(count(*) FILTER (WHERE PARTICIPANT_COUNT = 6 AND PATH_GAP_COUNT > 0) * 100.0
          / nullif(count(*) FILTER (WHERE PARTICIPANT_COUNT = 6), 0), 4) AS gap_p6_pct,
    round(count(*) FILTER (WHERE PATH_START_MATCH) * 100.0 / count(*), 4) AS start_match_pct,
    round(count(*) FILTER (WHERE PATH_END_MATCH) * 100.0 / count(*), 4) AS end_match_pct,
    round(count(*) FILTER (WHERE ANOMALY_TYPE IS NOT NULL) * 100.0 / count(*), 6) AS anomaly_pct,
    round(median(TOTAL_DURATION::DOUBLE), 1) AS median_duration_s
FROM combined
GROUP BY dataset;

CREATE OR REPLACE TABLE qa_sample_vs_full AS
SELECT
    metric,
    max(value) FILTER (WHERE dataset = 'full') AS full_value,
    max(value) FILTER (WHERE dataset = 'sample') AS sample_value,
    round(
        max(value) FILTER (WHERE dataset = 'sample')
        - max(value) FILTER (WHERE dataset = 'full'), 4
    ) AS diff_sample_minus_full,
    round(
        max(value) FILTER (WHERE dataset = 'sample')
        / nullif(max(value) FILTER (WHERE dataset = 'full'), 0), 4
    ) AS ratio_sample_to_full
FROM (
    UNPIVOT qa_sample_wide ON
        rows, ordering_accounts, ordering_countries, beneficiary_countries,
        currencies, local_currency_pct, bb_pct, bc_pct, cb_pct, cc_pct,
        direct_pct, indirect_pct, covered_pct, median_usd, median_bb_usd,
        median_bc_usd, median_cb_usd, median_cc_usd, gap_p2_pct, gap_p3_pct,
        gap_p4_pct, gap_p5_pct, gap_p6_pct, start_match_pct, end_match_pct,
        anomaly_pct, median_duration_s
    INTO NAME metric VALUE value
)
GROUP BY metric
ORDER BY metric;

COPY qa_checks TO 'data/aggregates/qa_checks.csv' (FORMAT CSV, HEADER);
COPY qa_sample_vs_full TO 'data/aggregates/qa_sample_vs_full.csv' (FORMAT CSV, HEADER);

SELECT * FROM qa_checks ORDER BY check_name;
SELECT * FROM qa_sample_vs_full ORDER BY metric;
