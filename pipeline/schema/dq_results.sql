CREATE TABLE IF NOT EXISTS dq.dq_report (
    -- Unique identifier for one Data Quality execution/run
    run_id          VARCHAR,
    -- Timestamp when the Data Quality execution was performed
    run_timestamp   TIMESTAMP,
    -- Name of the table being evaluated
    table_name      VARCHAR,
    -- Name of the column being evaluated
    column_name     VARCHAR,
    -- Data Quality dimension being evaluated
    -- e.g. Completeness, Validity, Uniqueness, Consistency
    dimension       VARCHAR,
    -- Specific metric used to measure the Data Quality dimension
    -- e.g. completeness_rate, validity_rate, uniqueness_rate, consistency_rate
    metric          VARCHAR,
    -- Total number of records evaluated by the DQ check
    total_records   BIGINT,
    -- Number of records that failed the DQ check
    failed_records  BIGINT,
    -- Result of the DQ metric, expressed as a percentage
    score           DECIMAL(5,2),
    -- Minimum acceptable score for the DQ check
    threshold       DECIMAL(5,2),
    -- Result of comparing the score against the threshold
    -- e.g. PASS or FAIL
    status          VARCHAR
);
