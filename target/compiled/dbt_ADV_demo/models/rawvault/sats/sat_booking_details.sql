

SELECT
  TO_HEX(MD5(cast(BookingID as string))) AS HK_BookingID,
  FlightID,PassengerID,Status,BookingDate,SeatNumber,SeatClass,PaymentID,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'AI_BOOKING_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`AI_BOOKING_DETAILS`

UNION ALL

SELECT
  TO_HEX(MD5(cast(BookingID as string))) AS HK_BookingID,
  FlightID,PassengerID,Status,BookingDate,SeatNumber,SeatClass,PaymentID,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'SJ_BOOKING_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`SJ_BOOKING_DETAILS`