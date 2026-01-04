

SELECT
  TO_HEX(MD5(cast(PassengerID as string))) AS HK_PassengerID,
  FirstName,LastName,DOB,Email,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'AI_PASSENGER_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`AI_PASSENGER_DETAILS`

UNION ALL

SELECT
  TO_HEX(MD5(cast(PassengerID as string))) AS HK_PassengerID,
  FirstName,LastName,DOB,Email,MobileNumber,CreatedDateTime,LastupdateDateTime,
  current_timestamp() as LOAD_DTS,
  'SJ_PASSENGER_DETAILS' as REC_SRC
FROM `dbthackathon`.`dbthackathon_dataset`.`SJ_PASSENGER_DETAILS`