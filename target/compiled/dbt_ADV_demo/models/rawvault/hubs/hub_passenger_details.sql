

select
  to_hex(md5(cast(PassengerID as string) || 'AI')) as HK_PassengerID,
  PassengerID,
  current_timestamp() as LOAD_DTS,
  'AI_PASSENGER_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`AI_PASSENGER_DETAILS`
where PassengerID is not null
group by PassengerID

union all

select
  to_hex(md5(cast(PassengerID as string) || 'SJ')) as HK_PassengerID,
  PassengerID,
  current_timestamp() as LOAD_DTS,
  'SJ_PASSENGER_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`SJ_PASSENGER_DETAILS`
where PassengerID is not null
group by PassengerID