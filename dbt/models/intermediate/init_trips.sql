with trips_alldata as(
  SELECT * 
  FROM {{ref('merge_yellow_green')}}
),

payment_types as(
  SELECT *
  FROM {{ref('payment_type_lookup')}}
),

enriched_data as (
SELECT 
  --Surrogate Key: make a unique key id 
  {{ dbt_utils.generate_surrogate_key([
    'a.vendor_id', 
    'a.pickup_datetime', 
    'a.pickup_location_id',
    'a.taxi_type']) }} as trip_id,
  
  --id:
  a.vendor_id,
  a.rate_code_id,

  --Location ID : 
  a.pickup_location_id,
  a.dropoff_location_id,

  --time:
  a.pickup_datetime,
  a.dropoff_datetime,
  a.pickup_month,

  --trip details:
  a.store_and_fwd_flag,
  a.passenger_count,
  a.trip_distance,
  a.trip_type,
  a.taxi_type,

  -- fee : 
  a.fare_amount,
  a.surcharge_amount,
  a.mta_tax,
  a.creditcard_tip_amount,
  a.tolls_amount,
  a.ehail_fee,
  a.improvement_surcharge,
  a.total_amount,
  a.airport_fee,
  
  --enrich payment_type
  coalesce(a.payment_type,5) as payment_type,
  coalesce(p.description,'Unknown') as payment_type_description

FROM {{ref('merge_yellow_green')}} a 
LEFT JOIN payment_types p
ON coalesce(a.payment_type,5) = p.payment_type

)


-- 現代寫法 (BigQuery)，deal with depulicate : 
SELECT *
FROM enriched_data
QUALIFY row_number() over(
  partition by
  vendor_id, pickup_datetime, pickup_location_id, taxi_type
  order by dropoff_datetime
   ) =1



