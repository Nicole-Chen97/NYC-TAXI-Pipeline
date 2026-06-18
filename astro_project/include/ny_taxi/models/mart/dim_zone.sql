--dimension table for NYC taxi zones
-- from seed file

SELECT 
locationid as location_id,
borough,
zone,
service_zone
from {{ref('taxi_zone_lookup')}}