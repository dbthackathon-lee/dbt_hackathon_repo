

SELECT
  TO_HEX(MD5(cast(PaymentID as string))) AS HK_PaymentID,
  PaymentMethod,Amount,TransactionDate,Status,
  current_timestamp() as LOAD_DTS,
  'AI_PAYMENT' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`AI_PAYMENT`

UNION ALL

SELECT
  TO_HEX(MD5(cast(PaymentID as string))) AS HK_PaymentID,
  PaymentMethod,Amount,TransactionDate,Status,
  current_timestamp() as LOAD_DTS,
  'SJ_PAYMENT' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`SJ_PAYMENT`