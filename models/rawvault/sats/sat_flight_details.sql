{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['sat'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_FlightID']
) }}

SELECT
  TO_HEX(MD5(cast(FlightID as string))) AS HK_FlightID,
  FlightNumber,ArrivalDateTime,ScheduledDepartureDateTime,ActualDepartureDateTime,OriginAirportCode,DestinationAirportCode,SeatCapacity,AvailableSeats,
  current_timestamp() as LOAD_DTS,
  'AI_FLIGHT_DETAILS' as REC_SRC
FROM {{ source('ai_source','AI_FLIGHT_DETAILS') }}

UNION ALL

SELECT
  TO_HEX(MD5(cast(FlightID as string))) AS HK_FlightID,
  FlightNumber,ArrivalDateTime,ScheduledDepartureDateTime,ActualDepartureDateTime,OriginAirportCode,DestinationAirportCode,SeatCapacity,AvailableSeats,
  current_timestamp() as LOAD_DTS,
  'SJ_FLIGHT_DETAILS' as REC_SRC
FROM {{ source('sj_source','SJ_FLIGHT_DETAILS') }}