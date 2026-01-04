{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['link'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_BOOKING_FLIGHT_PASSENGER_DETAILS']
) }}

select
  to_hex(md5(cast(BookingID as string) || cast(FlightID as string) || cast(PassengerID as string) || 'AI')) as HK_BOOKING_FLIGHT_PASSENGER_DETAILS,
  to_hex(md5(cast(BookingID as string) || 'AI')) as HK_BookingID,
  to_hex(md5(cast(FlightID as string) || 'AI')) as HK_FlightID,
  to_hex(md5(cast(PassengerID as string) || 'AI')) as HK_PassengerID,
  current_timestamp() as LOAD_DTS,
  'AI_BOOKING_DETAILS' as REC_SRC
from {{ source('ai_source','AI_BOOKING_DETAILS') }}

union all

select
  to_hex(md5(cast(BookingID as string) || cast(FlightID as string) || cast(PassengerID as string) || 'SJ')) as HK_BOOKING_FLIGHT_PASSENGER_DETAILS,
  to_hex(md5(cast(BookingID as string) || 'SJ')) as HK_BookingID,
  to_hex(md5(cast(FlightID as string) || 'SJ')) as HK_FlightID,
  to_hex(md5(cast(PassengerID as string) || 'SJ')) as HK_PassengerID,
  current_timestamp() as LOAD_DTS,
  'SJ_BOOKING_DETAILS' as REC_SRC
from {{ source('sj_source','SJ_BOOKING_DETAILS') }}