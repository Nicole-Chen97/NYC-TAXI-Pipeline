SELECT
pickup_month,
dropoff_zone,
SUM(total_amount) as airport_total_amount,
SUM(fare_amount) as  airport_net_fare,
COUNT(*) as airport_trips,
AVG(creditcard_tip_amount) as avg_tips,
AVG(trip_distance) as avg_distance



FROM {{ref('fct_taxi_trips')}}
where airport_fee>0
GROUP BY 1,2


