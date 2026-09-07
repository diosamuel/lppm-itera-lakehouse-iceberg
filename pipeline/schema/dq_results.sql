CREATE NAMESPACE IF NOT EXISTS default.dq;

CREATE TABLE IF NOT EXISTS dq.dq_report (
    -- Unique identifier for one Data Quality execution/run
    run_id          STRING,
    -- Timestamp when the Data Quality execution was performed
    run_timestamp   TIMESTAMP,
    -- Name of the table being evaluated
    table_name      STRING,
    -- Name of the column being evaluated
    column_name     STRING,
    -- Data Quality dimension being evaluated
    -- e.g. Completeness, Validity, Uniqueness, Consistency
    dimension       STRING,
    -- Specific metric used to measure the Data Quality dimension
    -- e.g. completeness_rate, validity_rate, uniqueness_rate, consistency_rate
    metric          STRING,
    -- Total number of records evaluated by the DQ check
    total_records   BIGINT,
    -- Number of records that failed the DQ check
    failed_records  BIGINT,
    -- Result of the DQ metric, expressed as a percentage
    score           DOUBLE,
    -- Minimum acceptable score for the DQ check
    threshold       DOUBLE,
    -- Result of comparing the score against the threshold
    -- e.g. PASS or FAIL
    status          STRING
);
