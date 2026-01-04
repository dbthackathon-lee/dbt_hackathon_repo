
  
    

    create or replace table `dbthackathon`.`dbthackathon_dataset_raw_vault`.`link_booking_flight_passenger_details`
      
    partition by load_dts
    cluster by HK_BOOKING_FLIGHT_PASSENGER_DETAILS

    
    OPTIONS()
    as (
      

select
  to_hex(md5(cast(BookingID as string) || cast(FlightID as string) || cast(PassengerID as string) || 'AI')) as HK_BOOKING_FLIGHT_PASSENGER_DETAILS,
  to_hex(md5(cast(BookingID as string) || 'AI')) as HK_BookingID,
  to_hex(md5(cast(FlightID as string) || 'AI')) as HK_FlightID,
  to_hex(md5(cast(PassengerID as string) || 'AI')) as HK_PassengerID,
  current_timestamp() as LOAD_DTS,
  'AI_BOOKING_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`AI_BOOKING_DETAILS`

union all

select
  to_hex(md5(cast(BookingID as string) || cast(FlightID as string) || cast(PassengerID as string) || 'SJ')) as HK_BOOKING_FLIGHT_PASSENGER_DETAILS,
  to_hex(md5(cast(BookingID as string) || 'SJ')) as HK_BookingID,
  to_hex(md5(cast(FlightID as string) || 'SJ')) as HK_FlightID,
  to_hex(md5(cast(PassengerID as string) || 'SJ')) as HK_PassengerID,
  current_timestamp() as LOAD_DTS,
  'SJ_BOOKING_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`SJ_BOOKING_DETAILS`
    );
  