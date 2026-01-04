{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['sat'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_PassengerID']
) }}

SELECT
  TO_HEX(MD5(cast(PassengerID as string))) AS HK_PassengerID,
  FirstName,LastName,DOB,Email,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'AI_PASSENGER_DETAILS' as REC_SRC
FROM {{ source('ai_source','AI_PASSENGER_DETAILS') }}

UNION ALL

SELECT
  TO_HEX(MD5(cast(PassengerID as string))) AS HK_PassengerID,
  FirstName,LastName,DOB,Email,MobileNumber,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'SJ_PASSENGER_DETAILS' as REC_SRC
FROM {{ source('sj_source','SJ_PASSENGER_DETAILS') }}