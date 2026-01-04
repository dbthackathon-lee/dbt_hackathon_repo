{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['hub'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_FlightID']
) }}

select
  to_hex(md5(cast(FlightID as string) || 'AI')) as HK_FlightID,
  FlightID,
  current_timestamp() as LOAD_DTS,
  'AI_FLIGHT_DETAILS' as REC_SRC
from {{ source('ai_source','AI_FLIGHT_DETAILS') }}
where FlightID is not null
group by FlightID

union all

select
  to_hex(md5(cast(FlightID as string) || 'SJ')) as HK_FlightID,
  FlightID,
  current_timestamp() as LOAD_DTS,
  'SJ_FLIGHT_DETAILS' as REC_SRC
from {{ source('sj_source','SJ_FLIGHT_DETAILS') }}
where FlightID is not null
group by FlightID