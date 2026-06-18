-- rpt_monthly_summary.sql
{{config(
    materialized='incremental', 
    unique_key=['report_month', 'taxi_type']
)}}


-- models/marts/mart_monthly_revenue.sql
SELECT
    pickup_month as report_month,
    taxi_type,
    
    -- Sales (營收端): 
    SUM(total_amount) as total_monthly_revenue,
    SUM(fare_amount) as net_fare, -- 核心車資收入
    
    -- Cost/Pass-through (代收成本): 
     -- 建議也把稅金列出，因為這也是過路財神
    SUM(tolls_amount) as total_tolls_amount,
    SUM(mta_tax) as total_mta_tax,
    SUM(airport_fee) as total_airport_fee,
    
    -- Duration and Trips (營運效率): 
    COUNT(*) as total_trips,
    
    -- 計算平均時程：BigQuery 需要使用 TIMESTAMP_DIFF 或先轉成秒
    AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) / 60) as avg_trip_duration_minutes,
    
    -- 修正拼字錯誤 ACG -> AVG
    AVG(trip_distance) as avg_trip_distance,
    
    -- 每單價值
    SAFE_DIVIDE(SUM(total_amount), COUNT(*)) as avg_revenue_per_trip
 
FROM {{ ref('fct_taxi_trips') }} 
WHERE payment_type NOT IN (4, 6) 
-- 注意：如果 payment_type 是字串，請加引號
-- 🚀 這是 Incremental 的核心：只抓比現有資料更新的月份
{% if is_incremental() %}
  AND EXTRACT(MONTH FROM pickup_datetime) = {{ var("target_month") }}
{% endif %}

GROUP BY 1, 2



