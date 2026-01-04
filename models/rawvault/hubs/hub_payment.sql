{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['hub'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_PaymentID']
) }}

select
  to_hex(md5(cast(PaymentID as string) || 'AI')) as HK_PaymentID,
  PaymentID,
  current_timestamp() as LOAD_DTS,
  'AI_PAYMENT' as REC_SRC
from {{ source('ai_source','AI_PAYMENT') }}
where PaymentID is not null
group by PaymentID

union all

select
  to_hex(md5(cast(PaymentID as string) || 'SJ')) as HK_PaymentID,
  PaymentID,
  current_timestamp() as LOAD_DTS,
  'SJ_PAYMENT' as REC_SRC
from {{ source('sj_source','SJ_PAYMENT') }}
where PaymentID is not null
group by PaymentID