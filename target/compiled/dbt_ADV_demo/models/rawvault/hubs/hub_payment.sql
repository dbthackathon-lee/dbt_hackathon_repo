

select
  to_hex(md5(cast(PaymentID as string) || 'AI')) as HK_PaymentID,
  PaymentID,
  current_timestamp() as LOAD_DTS,
  'AI_PAYMENT' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`AI_PAYMENT`
where PaymentID is not null
group by PaymentID

union all

select
  to_hex(md5(cast(PaymentID as string) || 'SJ')) as HK_PaymentID,
  PaymentID,
  current_timestamp() as LOAD_DTS,
  'SJ_PAYMENT' as REC_SRC
from `dbthackathon`.`dbthackathon_dataset`.`SJ_PAYMENT`
where PaymentID is not null
group by PaymentID