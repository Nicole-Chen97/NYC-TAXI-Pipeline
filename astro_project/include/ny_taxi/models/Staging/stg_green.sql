 -- 這裡用 view 即可，因為它只是 External Table 的分身
{{ config(
    materialized='view',
    tags=['green']
) }}



with source_data as (
    select * 
    from {{ source('raw_gcs_data', 'ext_green_taxi') }}
    -- 這裡的過濾發生在最內層，確保 BigQuery 只掃描該分區的檔案
    where year ={{ var("target_year") }}
      and month = {{ var("target_month") }}
),
filter_data as (
 SELECT
    CAST(vendor_id AS INT64) AS vendor_id,
    -- 將 NTZ 轉換為標準 TIMESTAMP
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(RatecodeID AS INT64) AS rate_code_id,
    CAST(pickup_location_id AS INT64) AS pickup_location_id,
    CAST(dropoff_location_id AS INT64) AS dropoff_location_id,
    CAST(passenger_count AS INT64) AS passenger_count,
    CAST(trip_distance AS FLOAT64) AS trip_distance,
    CAST(fare_amount AS FLOAT64) AS fare_amount,
    CAST(surcharge_amount AS FLOAT64) AS surcharge_amount,
    CAST(mta_tax AS FLOAT64) AS mta_tax,
    cast(creditcard_tip_amount as float64) as creditcard_tip_amount,
    CAST(tolls_amount AS FLOAT64) AS tolls_amount,
    CAST(ehail_fee AS FLOAT64) AS ehail_fee,
    CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
    CAST(total_amount AS FLOAT64) AS total_amount,
    CAST(payment_type AS INT64) AS payment_type,
    CAST(trip_type AS INT64) AS trip_type,
    CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
    -- CAST(cbd_congestion_fee AS FLOAT64) AS cbd_congestion_fee,
    CAST(taxi_type AS STRING) AS taxi_type,
    -- 如果 pickup_month 是 '2023-01' 這種格式，維持 String 或轉成 Date
   -- 在 staging 或 trips 表裡
    DATE_TRUNC(DATE(pickup_datetime), MONTH) AS pickup_month
   

  from source_data
  where vendor_id is not null
)


-- 關鍵：必須把資料選出來！
select * from filter_data


-- -- stg_green_taxi.sql
-- select * 
-- from {{ source('raw_gcs_data', 'ext_green_taxi') }}
-- where pickup_month = '2025-07' -- 或是根據變數動態篩選