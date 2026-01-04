{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['sat'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_BookingID']
) }}

SELECT
  TO_HEX(MD5(cast(BookingID as string))) AS HK_BookingID,
  FlightID,PassengerID,Status,BookingDate,SeatNumber,SeatClass,PaymentID,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'AI_BOOKING_DETAILS' as REC_SRC
FROM {{ source('ai_source','AI_BOOKING_DETAILS') }}

UNION ALL

SELECT
  TO_HEX(MD5(cast(BookingID as string))) AS HK_BookingID,
  FlightID,PassengerID,Status,BookingDate,SeatNumber,SeatClass,PaymentID,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'SJ_BOOKING_DETAILS' as REC_SRC
FROM {{ source('sj_source','SJ_BOOKING_DETAILS') }}