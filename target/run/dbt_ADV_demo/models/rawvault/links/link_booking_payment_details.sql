
  
    

    create or replace table `dbthackathon`.`dbthackathon_dataset_raw_vault`.`link_booking_payment_details`
      
    partition by load_dts
    cluster by HK_BOOKING_PAYMENT_DETAILS

    
    OPTIONS()
    as (
      

select
  to_hex(md5(cast(BookingID as string) || cast(PaymentID as string) || 'AI')) as HK_BOOKING_PAYMENT_DETAILS,
  to_hex(md5(cast(BookingID as string) || 'AI')) as HK_BookingID,
  to_hex(md5(cast(PaymentID as string) || 'AI')) as HK_PaymentID,
  current_timestamp() as LOAD_DTS,
  'AI_BOOKING_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`AI_BOOKING_DETAILS`

union all

select
  to_hex(md5(cast(BookingID as string) || cast(PaymentID as string) || 'SJ')) as HK_BOOKING_PAYMENT_DETAILS,
  to_hex(md5(cast(BookingID as string) || 'SJ')) as HK_BookingID,
  to_hex(md5(cast(PaymentID as string) || 'SJ')) as HK_PaymentID,
  current_timestamp() as LOAD_DTS,
  'SJ_BOOKING_DETAILS' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`SJ_BOOKING_DETAILS`
    );
  