{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['hub'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_PassengerID']
) }}

select
  to_hex(md5(cast(PassengerID as string) || 'AI')) as HK_PassengerID,
  PassengerID,
  current_timestamp() as LOAD_DTS,
  'AI_PASSENGER_DETAILS' as REC_SRC
from {{ source('ai_source','AI_PASSENGER_DETAILS') }}
where PassengerID is not null
group by PassengerID

union all

select
  to_hex(md5(cast(PassengerID as string) || 'SJ')) as HK_PassengerID,
  PassengerID,
  current_timestamp() as LOAD_DTS,
  'SJ_PASSENGER_DETAILS' as REC_SRC
from {{ source('sj_source','SJ_PASSENGER_DETAILS') }}
where PassengerID is not null
group by PassengerID