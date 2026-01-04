{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['sat'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_PaymentID']
) }}

SELECT
  TO_HEX(MD5(cast(PaymentID as string))) AS HK_PaymentID,
  PaymentMethod,Amount,TransactionDate,Status,
  current_timestamp() as LOAD_DTS,
  'AI_PAYMENT' as REC_SRC
FROM {{ source('ai_source','AI_PAYMENT') }}

UNION ALL

SELECT
  TO_HEX(MD5(cast(PaymentID as string))) AS HK_PaymentID,
  PaymentMethod,Amount,TransactionDate,Status,
  current_timestamp() as LOAD_DTS,
  'SJ_PAYMENT' as REC_SRC
FROM {{ source('sj_source','SJ_PAYMENT') }}