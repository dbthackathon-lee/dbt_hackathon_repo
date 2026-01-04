
  
    

    create or replace table `dbthackathon`.`dbthackathon_dataset_raw_vault`.`sat_airport_details`
      
    partition by load_dts
    cluster by HK_AirportCode

    
    OPTIONS()
    as (
      

SELECT
  TO_HEX(MD5(cast(AirportCode as string))) AS HK_AirportCode,
  AirportName,
  current_timestamp() as LOAD_DTS,
  'AI_AIRPORT_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`AI_AIRPORT_DETAILS`

UNION ALL

SELECT
  TO_HEX(MD5(cast(AirportCode as string))) AS HK_AirportCode,
  AirportName,
  current_timestamp() as LOAD_DTS,
  'SJ_AIRPORT_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`SJ_AIRPORT_DETAILS`
    );
  