{{ config(
    materialized='table',
    schema='raw_vault',
    tags=['sat'],
    partition_by={'field': 'LOAD_DTS'},
    cluster_by=['HK_AirportCode']
) }}

SELECT
  TO_HEX(MD5(cast(AirportCode as string))) AS HK_AirportCode,
  AirportName,
  current_timestamp() as LOAD_DTS,
  'AI_AIRPORT_DETAILS' as REC_SRC
FROM {{ source('ai_source','AI_AIRPORT_DETAILS') }}

UNION ALL

SELECT
  TO_HEX(MD5(cast(AirportCode as string))) AS HK_AirportCode,
  AirportName,
  current_timestamp() as LOAD_DTS,
  'SJ_AIRPORT_DETAILS' as REC_SRC
FROM {{ source('sj_source','SJ_AIRPORT_DETAILS') }}