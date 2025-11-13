
DROP TABLE IF EXISTS `the1-insight-dev.insight_dev.ms_personas` ;
CREATE OR REPLACE TABLE `the1-insight-dev.insight_dev.ms_personas` (
      accountId STRING
    , dateOfBirth DATE
    , gender STRING
    , hasEmail STRING
    , hasMobile STRING
    , languagePrefer STRING
    , memberId STRING NOT NULL OPTIONS(description="Primary key")
    , nationalityId STRING
    , profileId STRING
)
CLUSTER BY member_number
-- projects/120574803/locations/asia-southeast1/connections/insight_data_pipeline_biglake_connection
WITH CONNECTION `the1-insight-dev.asia-southeast1.insight_data_pipeline_biglake_connection`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://t1-insight-data-bucket/iceberg/ms_personas/',
  file_format = 'PARQUET'
  -- partition_granularity = 'HOUR'  -- สำหรับ Time Travel resolution ที่ละเอียด
);

-- -- Enable CDC writes
ALTER TABLE `the1-insight-dev.insight_dev.ms_personas`
ADD PRIMARY KEY (member_number) NOT ENFORCED;

-- CREATE OR REPLACE TABLE `the1-insight-dev.insight_dev.ms_personas_iceberg` (
--       member_id STRING
--     , member_number STRING NOT NULL OPTIONS(description="Primary key")
--     , nationality STRING
--     , country STRING
--     , passport_exp DATE
--     , birth_date DATE
--     , age STRING
--     , mobile_country_code STRING
--     , home_ph_country_code STRING
--     , type_of_housing STRING
--     , sub_district STRING
--     , district STRING
--     , city STRING
--     , postal_code STRING
--     , member_type STRING
--     , status_code STRING
--     , hold_reason STRING
--     , register_date TIMESTAMP
--     , member_ref_by_name STRING
--     , member_ref_by_id STRING
--     , register_channel STRING
--     , register_partner STRING
--     , gender STRING
--     , marital_status STRING
--     , job_title STRING
--     , education STRING
--     , monthly_income STRING
--     , prefer_lang STRING
--     , customer_type STRING
--     , register_staff STRING
--     , register_branch STRING
--     , privacy_flag STRING
--     , data_invalid_flag STRING
--     , dummy_flag STRING
--     , employee_bu_group STRING
--     , employee_bu STRING
--     , employee_resign_date TIMESTAMP
--     , employee_join_date DATE
--     , employee_id STRING
--     , created_date TIMESTAMP
--     , member_number_merged STRING
--     , register_partner_code STRING
--     , register_branch_code STRING
--     , is_address STRING
--     , is_mobile STRING
--     , is_email STRING
--     , is_send_sms_eng STRING
--     , is_send_sms_thai STRING
--     , is_expate STRING
--     , is_cds_line STRING
--     , is_rbs_line STRING
--     , is_ssp_line STRING
--     , is_cpn_line STRING
--     , is_cfr_line STRING
--     , is_cfm_line STRING
--     , is_twd_line STRING
--     , is_the1_line STRING
--     , updated_date TIMESTAMP
--     , insurance_not_send STRING
--     , consent_flag STRING
--     , consent_channel STRING
--     , consent_version STRING
--     , consent_date TIMESTAMP
--     , iscall STRING
--     , is_call_cds STRING
--     , is_email_cds STRING
--     , is_address_cds STRING
--     , is_send_sms_eng_cds STRING
--     , is_send_sms_thai_cds STRING
--     , is_call_rbs STRING
--     , is_email_rbs STRING
--     , is_address_rbs STRING
--     , is_send_sms_eng_rbs STRING
--     , is_send_sms_thai_rbs STRING
--     , is_call_b2s STRING
--     , is_email_b2s STRING
--     , is_address_b2s STRING
--     , is_send_sms_eng_b2s STRING
--     , is_send_sms_thai_b2s STRING
--     , is_call_hws STRING
--     , is_email_hws STRING
--     , is_address_hws STRING
--     , is_send_sms_eng_hws STRING
--     , is_send_sms_thai_hws STRING
--     , is_call_twd STRING
--     , is_email_twd STRING
--     , is_address_twd STRING
--     , is_send_sms_eng_twd STRING
--     , is_send_sms_thai_twd STRING
--     , is_call_ssp STRING
--     , is_email_ssp STRING
--     , is_address_ssp STRING
--     , is_send_sms_eng_ssp STRING
--     , is_send_sms_thai_ssp STRING
--     , is_call_pwb STRING
--     , is_email_pwb STRING
--     , is_address_pwb STRING
--     , is_send_sms_eng_pwb STRING
--     , is_send_sms_thai_pwb STRING
--     , is_call_ofm STRING
--     , is_email_ofm STRING
--     , is_address_ofm STRING
--     , is_send_sms_eng_ofm STRING
--     , is_send_sms_thai_ofm STRING
--     , is_call_cfm STRING
--     , is_email_cfm STRING
--     , is_address_cfm STRING
--     , is_send_sms_eng_cfm STRING
--     , is_send_sms_thai_cfm STRING
--     , is_call_cfr STRING
--     , is_email_cfr STRING
--     , is_address_cfr STRING
--     , is_send_sms_eng_cfr STRING
--     , is_send_sms_thai_cfr STRING
--     , is_call_cmg STRING
--     , is_email_cmg STRING
--     , is_address_cmg STRING
--     , is_send_sms_eng_cmg STRING
--     , is_send_sms_thai_cmg STRING
--     , th_title STRING
--     , eng_title STRING
--     , ever_consent_partner STRING
--     , is_consent_the1 STRING
--     , etl_created_by STRING
--     , etl_created_tms STRING
--     , invalid_member_flag STRING
--     , invalid_type STRING
--     -- , processed_at TIMESTAMP
--     -- , _change_type STRING
--     -- , _sequence_number INT64
-- )
-- CLUSTER BY member_number
-- WITH CONNECTION `the1-insight-dev.asia-southeast1.biglake_connection`
-- OPTIONS (
--   table_format = 'ICEBERG',
--   storage_uri = 'gs://t1-insight-data-bucket/iceberg/ms_personas/',
--   file_format = 'PARQUET',
--   partition_granularity = 'HOUR'  -- สำหรับ Time Travel resolution ที่ละเอียด
-- );

-- -- Enable CDC writes
-- ALTER TABLE `the1-insight-dev.insight_dev.ms_personas_iceberg`
-- ADD PRIMARY KEY (member_number) NOT ENFORCED;
