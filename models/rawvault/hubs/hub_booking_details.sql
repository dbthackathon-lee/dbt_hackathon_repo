{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['hub'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_BookingID']
) }}

select
  to_hex(md5(cast(BookingID as string) || 'AI')) as HK_BookingID,
  BookingID,
  current_timestamp() as LOAD_DTS,
  'AI_BOOKING_DETAILS' as REC_SRC
from {{ source('ai_source','AI_BOOKING_DETAILS') }}
where BookingID is not null
group by BookingID

union all

select
  to_hex(md5(cast(BookingID as string) || 'SJ')) as HK_BookingID,
  BookingID,
  current_timestamp() as LOAD_DTS,
  'SJ_BOOKING_DETAILS' as REC_SRC
from {{ source('sj_source','SJ_BOOKING_DETAILS') }}
where BookingID is not null
group by BookingID