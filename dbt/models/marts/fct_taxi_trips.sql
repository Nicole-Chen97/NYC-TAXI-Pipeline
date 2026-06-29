--increase in init_trips.sql
-- incremental_strategy='merge',     重跑時自動覆蓋舊資料
-- on_schema_change='append_new_columns',  欄位變動自動處理
--"data_type": "date",    這裡現在匹配了！


{{
  config(
    materialized='incremental',
    unique_key='trip_id',  
    on_schema_change='append_new_columns',
    partition_by={
      "field": "pickup_month",
      "data_type": "date",    
      "granularity": "month"
    }
  )
}}

  -- 增量邏輯：只抓取比現有資料更新的月份，節省費用
      --{{ this }}：代表「這張表自己」

with trips_data as(
  select *
  from {{ref('init_trips')}}
  {% if is_incremental() %}
    
      WHERE EXTRACT(MONTH FROM pickup_datetime) = {{ var("target_month") }}
    {% endif %}
)
,
dim_zone as (
  select*

  from {{ref('dim_zone')}}
)
,
fact_table as(
  select
    t.trip_id,
    --id:
    t.vendor_id,
    t.rate_code_id,

    --Location ID : 
    t.pickup_location_id,
    t.dropoff_location_id,

    --time:
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_month,

    --trip details:
    t.store_and_fwd_flag,
    t.passenger_count,
    t.trip_distance,
    t.trip_type,
    t.taxi_type,

    -- fee : 
    t.fare_amount,
    t.surcharge_amount,
    t.mta_tax,
    t.creditcard_tip_amount,
    t.tolls_amount,
    t.ehail_fee,
    t.improvement_surcharge,
    t.total_amount,
    t.airport_fee,

  --payment:
    t.payment_type,
    t.payment_type_description,

  --enrich :
  -- pickup info:
    pz.borough AS pickup_borough,
    pz.zone AS pickup_zone,

  --dropoff:
    dz.borough AS dropoff_borough,
    dz.zone AS dropoff_zone

  FROM trips_data as t 

  LEFT JOIN dim_zone pz
  ON t.pickup_location_id = pz.location_id

  LEFT JOIN dim_zone dz
  ON t.dropoff_location_id = dz.location_id
)

select * from fact_table




