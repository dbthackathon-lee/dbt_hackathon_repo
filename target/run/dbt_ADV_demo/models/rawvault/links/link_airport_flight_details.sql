
  
    

    create or replace table `dbthackathon`.`dbthackathon_dataset_raw_vault`.`link_airport_flight_details`
      
    partition by load_dts
    cluster by HK_AIRPORT_FLIGHT_DETAILS

    
    OPTIONS()
    as (
      

select
  to_hex(md5(cast(FlightID as string) || cast(OriginAirportCode as string) || cast(DestinationAirportCode as string) || 'AI')) as HK_AIRPORT_FLIGHT_DETAILS,
  to_hex(md5(cast(FlightID as string) || 'AI')) as HK_FlightID,
  to_hex(md5(cast(OriginAirportCode as string) || 'AI')) as HK_OriginAirportCode,
  to_hex(md5(cast(DestinationAirportCode as string) || 'AI')) as HK_DestinationAirportCode,
  current_timestamp() as LOAD_DTS,
  'AI_FLIGHT_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`AI_FLIGHT_DETAILS`

union all

select
  to_hex(md5(cast(FlightID as string) || cast(OriginAirportCode as string) || cast(DestinationAirportCode as string) || 'SJ')) as HK_AIRPORT_FLIGHT_DETAILS,
  to_hex(md5(cast(FlightID as string) || 'SJ')) as HK_FlightID,
  to_hex(md5(cast(OriginAirportCode as string) || 'SJ')) as HK_OriginAirportCode,
  to_hex(md5(cast(DestinationAirportCode as string) || 'SJ')) as HK_DestinationAirportCode,
  current_timestamp() as LOAD_DTS,
  'SJ_FLIGHT_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`SJ_FLIGHT_DETAILS`
    );
  