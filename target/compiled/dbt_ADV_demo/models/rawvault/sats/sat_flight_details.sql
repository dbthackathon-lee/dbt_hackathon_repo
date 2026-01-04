

SELECT
  TO_HEX(MD5(cast(FlightID as string))) AS HK_FlightID,
  FlightNumber,ArrivalDateTime,ScheduledDepartureDateTime,ActualDepartureDateTime,OriginAirportCode,DestinationAirportCode,SeatCapacity,AvailableSeats,
  current_timestamp() as LOAD_DTS,
  'AI_FLIGHT_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`AI_FLIGHT_DETAILS`

UNION ALL

SELECT
  TO_HEX(MD5(cast(FlightID as string))) AS HK_FlightID,
  FlightNumber,ArrivalDateTime,ScheduledDepartureDateTime,ActualDepartureDateTime,OriginAirportCode,DestinationAirportCode,SeatCapacity,AvailableSeats,
  current_timestamp() as LOAD_DTS,
  'SJ_FLIGHT_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`SJ_FLIGHT_DETAILS`