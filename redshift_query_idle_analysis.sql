-- Redshift Query Gap Analysis - Accurate Idle Time Calculation
-- This query calculates idle time by analyzing gaps between individual queries
-- Provides accurate idle percentage by considering all gaps between queries

WITH analysis_window AS (
    SELECT 
        DATEADD(day, -1, GETDATE()) AS start_time,
        GETDATE() AS end_time,
        DATEDIFF(second, DATEADD(day, -1, GETDATE()), GETDATE()) AS total_seconds
),
query_timeline AS (
    SELECT 
        sqh.start_time,
        sqh.end_time,
        sqh.status,
        sqh.elapsed_time / 1000000.0 AS execution_seconds,
        sqh.queue_time / 1000000.0 AS queue_seconds,
        -- Calculate gap to next query
        LEAD(sqh.start_time) OVER (ORDER BY sqh.start_time) AS next_query_start,
        ROW_NUMBER() OVER (ORDER BY sqh.start_time) AS query_sequence
    FROM sys_query_history sqh, analysis_window aw
    WHERE sqh.start_time >= aw.start_time
        AND sqh.start_time <= aw.end_time
),
query_gaps AS (
    SELECT 
        start_time,
        end_time,
        execution_seconds,
        queue_seconds,
        status,
        query_sequence,
        -- Calculate gap duration to next query
        CASE 
            WHEN next_query_start IS NOT NULL 
            THEN DATEDIFF(second, end_time, next_query_start)
            ELSE 0
        END AS gap_to_next_seconds
    FROM query_timeline
),
gap_summary AS (
    SELECT 
        COUNT(*) AS total_queries,
        COUNT(CASE WHEN status = 'success' THEN 1 END) AS successful_queries,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed_queries,
        COUNT(CASE WHEN status = 'aborted' THEN 1 END) AS aborted_queries,
        SUM(execution_seconds) AS total_execution_seconds,
        SUM(queue_seconds) AS total_queue_seconds,
        SUM(gap_to_next_seconds) AS total_gap_seconds,
        MIN(start_time) AS first_query_time,
        MAX(end_time) AS last_query_time,
        -- Calculate time before first query and after last query
        DATEDIFF(second, (SELECT start_time FROM analysis_window), MIN(start_time)) AS time_before_first_query,
        DATEDIFF(second, MAX(end_time), (SELECT end_time FROM analysis_window)) AS time_after_last_query
    FROM query_gaps
),
idle_calculation AS (
    SELECT 
        aw.total_seconds,
        gs.*,
        -- Total gaps between queries + time outside query period
        gs.total_gap_seconds + gs.time_before_first_query + gs.time_after_last_query AS total_idle_seconds,
        -- Calculate percentage
        ((gs.total_gap_seconds + gs.time_before_first_query + gs.time_after_last_query) * 100.0) / aw.total_seconds AS idle_percentage
    FROM analysis_window aw, gap_summary gs
)

-- Final Results
SELECT 
    '=== REDSHIFT QUERY-LEVEL IDLE ANALYSIS ===' AS metric,
    NULL AS value,
    NULL AS unit
UNION ALL
SELECT 
    'Analysis Period' AS metric,
    ROUND(total_seconds / 3600.0, 2)::varchar AS value,
    'hours' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Total Queries' AS metric,
    total_queries::varchar AS value,
    'queries' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Successful Queries' AS metric,
    successful_queries::varchar AS value,
    'queries' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Failed Queries' AS metric,
    failed_queries::varchar AS value,
    'queries' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Aborted Queries' AS metric,
    aborted_queries::varchar AS value,
    'queries' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Total Execution Time' AS metric,
    ROUND(total_execution_seconds / 3600.0, 4)::varchar AS value,
    'hours' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Total Queue Time' AS metric,
    ROUND(total_queue_seconds / 3600.0, 4)::varchar AS value,
    'hours' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Gaps Between Queries' AS metric,
    ROUND(total_gap_seconds / 3600.0, 2)::varchar AS value,
    'hours' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Time Before First Query' AS metric,
    ROUND(time_before_first_query / 3600.0, 2)::varchar AS value,
    'hours' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Time After Last Query' AS metric,
    ROUND(time_after_last_query / 3600.0, 2)::varchar AS value,
    'hours' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Total Idle Time' AS metric,
    ROUND(total_idle_seconds / 3600.0, 2)::varchar AS value,
    'hours' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Idle Percentage' AS metric,
    ROUND(idle_percentage, 2)::varchar AS value,
    '%' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'First Query Time' AS metric,
    COALESCE(first_query_time::varchar, 'No queries') AS value,
    '' AS unit
FROM idle_calculation
UNION ALL
SELECT 
    'Last Query Time' AS metric,
    COALESCE(last_query_time::varchar, 'No queries') AS value,
    '' AS unit
FROM idle_calculation;