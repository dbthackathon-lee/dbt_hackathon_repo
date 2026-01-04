
  
    

    create or replace table `dbthackathon`.`dbthackathon_dataset_raw_vault`.`hub_flight_details`
      
    partition by load_dts
    cluster by HK_FlightID

    
    OPTIONS()
    as (
      

select
  to_hex(md5(cast(FlightID as string) || 'AI')) as HK_FlightID,
  FlightID,
  current_timestamp() as LOAD_DTS,
  'AI_FLIGHT_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`AI_FLIGHT_DETAILS`
where FlightID is not null
group by FlightID

union all

select
  to_hex(md5(cast(FlightID as string) || 'SJ')) as HK_FlightID,
  FlightID,
  current_timestamp() as LOAD_DTS,
  'SJ_FLIGHT_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`SJ_FLIGHT_DETAILS`
where FlightID is not null
group by FlightID
    );
  