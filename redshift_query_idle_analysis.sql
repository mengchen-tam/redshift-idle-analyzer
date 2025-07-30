-- Redshift Query-Level Idle Time Analysis
-- This query analyzes idle time based on query activity spans (conservative approach)
-- Complements the IO-level analysis provided by redshift_idle_calculator.py

WITH analysis_window AS (
    SELECT 
        DATEADD(day, -1, GETDATE()) AS start_time,
        GETDATE() AS end_time,
        DATEDIFF(second, DATEADD(day, -1, GETDATE()), GETDATE()) AS total_seconds
),
query_summary AS (
    SELECT 
        COUNT(*) AS total_queries,
        COUNT(CASE WHEN sqh.status = 'success' THEN 1 END) AS successful_queries,
        COUNT(CASE WHEN sqh.status = 'failed' THEN 1 END) AS failed_queries,
        COUNT(CASE WHEN sqh.status = 'aborted' THEN 1 END) AS aborted_queries,
        SUM(sqh.elapsed_time) / 1000000.0 AS total_execution_seconds,
        SUM(sqh.queue_time) / 1000000.0 AS total_queue_seconds,
        MIN(sqh.start_time) AS first_query_time,
        MAX(sqh.end_time) AS last_query_time
    FROM sys_query_history sqh, analysis_window aw
    WHERE sqh.start_time >= aw.start_time
        AND sqh.start_time <= aw.end_time
),
idle_analysis AS (
    SELECT 
        aw.total_seconds,
        qs.total_queries,
        qs.successful_queries,
        qs.failed_queries,
        qs.aborted_queries,
        qs.total_execution_seconds,
        qs.total_queue_seconds,
        qs.first_query_time,
        qs.last_query_time,
        -- Calculate query span (from first query to last query)
        CASE 
            WHEN qs.first_query_time IS NOT NULL AND qs.last_query_time IS NOT NULL
            THEN DATEDIFF(second, qs.first_query_time, qs.last_query_time)
            ELSE 0
        END AS query_span_seconds,
        -- Calculate idle time (time outside of query span)
        CASE 
            WHEN qs.first_query_time IS NOT NULL AND qs.last_query_time IS NOT NULL
            THEN aw.total_seconds - DATEDIFF(second, qs.first_query_time, qs.last_query_time)
            ELSE aw.total_seconds
        END AS idle_seconds,
        -- Calculate idle percentage
        CASE 
            WHEN qs.first_query_time IS NOT NULL AND qs.last_query_time IS NOT NULL
            THEN ((aw.total_seconds - DATEDIFF(second, qs.first_query_time, qs.last_query_time)) * 100.0) / aw.total_seconds
            ELSE 100.0
        END AS idle_percentage
    FROM analysis_window aw, query_summary qs
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
FROM idle_analysis
UNION ALL
SELECT 
    'Total Queries' AS metric,
    total_queries::varchar AS value,
    'queries' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Successful Queries' AS metric,
    successful_queries::varchar AS value,
    'queries' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Failed Queries' AS metric,
    failed_queries::varchar AS value,
    'queries' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Aborted Queries' AS metric,
    aborted_queries::varchar AS value,
    'queries' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Query Span (First to Last)' AS metric,
    ROUND(query_span_seconds / 3600.0, 2)::varchar AS value,
    'hours' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Total Execution Time' AS metric,
    ROUND(total_execution_seconds / 3600.0, 4)::varchar AS value,
    'hours' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Total Queue Time' AS metric,
    ROUND(total_queue_seconds / 3600.0, 4)::varchar AS value,
    'hours' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Idle Time (Conservative)' AS metric,
    ROUND(idle_seconds / 3600.0, 2)::varchar AS value,
    'hours' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Idle Percentage (Conservative)' AS metric,
    ROUND(idle_percentage, 2)::varchar AS value,
    '%' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'First Query Time' AS metric,
    COALESCE(first_query_time::varchar, 'No queries in period') AS value,
    '' AS unit
FROM idle_analysis
UNION ALL
SELECT 
    'Last Query Time' AS metric,
    COALESCE(last_query_time::varchar, 'No queries in period') AS value,
    '' AS unit
FROM idle_analysis;