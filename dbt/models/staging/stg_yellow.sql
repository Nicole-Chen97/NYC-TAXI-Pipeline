-- models/staging/stg_yellow_taxi.sql

-- Staging model : recommand use View to save space and improve performance speed
-- 這裡用 view 即可，因為它只是 External Table 的分身
{{ config(
    materialized='view', 
    tags=['yellow']
) }}


with source_data as (
    select * 
    from {{ source('raw_gcs_data', 'ext_yellow_taxi') }}
    -- 這裡的過濾發生在最內層，確保 BigQuery 只掃描該分區的檔案
    where year = {{ var("target_year") }}
      and month = {{ var("target_month") }}
),

filter_data as (
  select 
    -- [型態對齊] 根據你的 Spark Schema 進行轉型
    -- ID 建議轉 String 以防科學記號
    cast(vendor_id as INT64) as vendor_id, 
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- 整數型態 (Long/Integer -> INT64)
    cast(passenger_count as int64) as passenger_count,
    cast(RatecodeID as int64) as rate_code_id,
    cast(pickup_location_id as int64) as pickup_location_id,
    cast(dropoff_location_id as int64) as dropoff_location_id,
    cast(payment_type as int64) as payment_type,

    -- 浮點數型態 (Double -> FLOAT64)
    cast(trip_distance as float64) as trip_distance,
    cast(fare_amount as float64) as fare_amount,
    cast(surcharge_amount as float64) as surcharge_amount,
    cast(mta_tax as float64) as mta_tax,
    cast(creditcard_tip_amount as float64) as creditcard_tip_amount,
    cast(tolls_amount as float64) as tolls_amount,
    cast(improvement_surcharge as float64) as improvement_surcharge,
    cast(total_amount as float64) as total_amount,
    cast(congestion_surcharge as float64) as congestion_surcharge,
    cast(Airport_fee as float64) as airport_fee,
    -- cast(cbd_congestion_fee as float64) as cbd_congestion_fee,

    -- 字串型態
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    CAST(taxi_type AS STRING) AS taxi_type,

    -- [分區欄位] 假設你在 Spark 有做 pickup_month
    DATE_TRUNC(DATE(pickup_datetime), MONTH) AS pickup_month
  from source_data
  where vendor_id is not null
)

-- 關鍵：必須把資料選出來！
select * from filter_data