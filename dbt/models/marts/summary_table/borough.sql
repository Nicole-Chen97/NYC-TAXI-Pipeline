
{{config(
    materialized='incremental', 
    unique_key=['report_month', 'pickup_borough', 'trip_category']
)}}

SELECT
    -- 空間維度 (如果你有 Join dim_zones，建議用 Borough 或 Zone Name)
    pickup_month as report_month,
    pickup_borough,
    CASE WHEN airport_fee > 0 THEN 'Airport' ELSE 'City' END as trip_category,
    
    -- 1. 需求熱點
    COUNT(*) as pickup_count,
    
    -- 2. 獲利能力 (平均每趟賺錢)
    AVG(fare_amount) as avg_fare_per_trip, -- 核心車資
    AVG(total_amount) as avg_total_per_trip, -- 含規費的總額

    -- 效率部分
    AVG(trip_distance) as avg_distance,
    AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)) as avg_duration_min,
    
    -- 3. 小費分析 (只算信用卡，避免被現金 0 稀釋)
    --SAFE_DIVIDE: 防止分母為0
    AVG(CASE WHEN payment_type = 1 THEN creditcard_tip_amount ELSE NULL END) as avg_credit_tip,
    SAFE_DIVIDE(
        SUM(CASE WHEN payment_type = 1 THEN creditcard_tip_amount ELSE 0 END),
        SUM(CASE WHEN payment_type = 1 THEN fare_amount ELSE 0 END)
    ) as tip_rate,
    
    -- 4. 機場營收概況
    SUM(CASE WHEN airport_fee > 0 THEN total_amount ELSE 0 END) as total_airport_revenue,
    COUNT(CASE WHEN airport_fee > 0 THEN 1 END) as airport_pickup_count

FROM {{ ref('fct_taxi_trips') }}
WHERE payment_type NOT IN (4, 6) 

-- 排除爭議與作廢

-- 🚀 這是 Incremental 的核心：只抓比現有資料更新的月份
{% if is_incremental() %}
  AND pickup_month >= (SELECT MAX(report_month) FROM {{ this }})
{% endif %}

GROUP BY 1,2, 3