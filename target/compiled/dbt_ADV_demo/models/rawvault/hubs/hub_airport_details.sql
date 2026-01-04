

select
  to_hex(md5(cast(AirportCode as string) || 'AI')) as HK_AirportCode,
  AirportCode,
  current_timestamp() as LOAD_DTS,
  'AI_AIRPORT_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`AI_AIRPORT_DETAILS`
where AirportCode is not null
group by AirportCode

union all

select
  to_hex(md5(cast(AirportCode as string) || 'SJ')) as HK_AirportCode,
  AirportCode,
  current_timestamp() as LOAD_DTS,
  'SJ_AIRPORT_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`SJ_AIRPORT_DETAILS`
where AirportCode is not null
group by AirportCode