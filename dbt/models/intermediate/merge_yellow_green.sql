---- Union green and yellow taxi data into a single dataset
 -- Green Taxi 沒有這個費率，補 0
 -- Yellow taxis only do street-hail (code 1)
  -- Yellow taxis don't have ehail_fee
with green_trips as(
  select 
   vendor_id,
   rate_code_id,
   pickup_location_id,
   dropoff_location_id,
   pickup_datetime,
   dropoff_datetime,
   store_and_fwd_flag,
   passenger_count,
   trip_distance,
   trip_type,
   fare_amount,
   surcharge_amount,
   mta_tax,
   creditcard_tip_amount,
   tolls_amount,
   ehail_fee,
   improvement_surcharge,
   total_amount,
   payment_type,
   pickup_month,
   cast(0 as FLOAT64) as airport_fee,  
   taxi_type
   from {{ref('stg_green')}}
),
yellow_trips as(
  select 
   vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    cast(1 as INT64) as trip_type,  
    fare_amount,
    surcharge_amount,
    mta_tax,
    creditcard_tip_amount,
    tolls_amount,
    cast(0 as FLOAT64) as ehail_fee, 
    improvement_surcharge,
    total_amount,
    payment_type,
    pickup_month,
    airport_fee,
    taxi_type

   from {{ref('stg_yellow')}}
)

 
select * from green_trips
UNION ALL 
select * from yellow_trips