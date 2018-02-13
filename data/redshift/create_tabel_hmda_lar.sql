 CREATE EXTERNAL TABLE hmda.hmda_lar(
  action_taken int, 
  action_taken_name varchar(2000), 
  agency_code int, 
  agency_abbr varchar(2000), 
  agency_name varchar(2000), 
  applicant_ethnicity int, 
  applicant_ethnicity_name varchar(2000), 
  applicant_income_000s int, 
  applicant_race_1 int, 
  applicant_race_2 int, 
  applicant_race_3 int, 
  applicant_race_4 int, 
  applicant_race_5 int, 
  applicant_race_name_1 varchar(2000), 
  applicant_race_name_2 varchar(2000), 
  applicant_race_name_3 varchar(2000), 
  applicant_race_name_4 varchar(2000), 
  applicant_race_name_5 varchar(2000), 
  applicant_sex int, 
  applicant_sex_name varchar(2000), 
  application_date_indicator int, 
  census_tract_number varchar(2000), 
  co_applicant_ethnicity int, 
  co_applicant_ethnicity_name int, 
  co_applicant_race_1 int, 
  co_applicant_race_2 int, 
  co_applicant_race_3 int, 
  co_applicant_race_4 int, 
  co_applicant_race_5 int, 
  co_applicant_race_name_1 varchar(2000), 
  co_applicant_race_name_2 varchar(2000), 
  co_applicant_race_name_3 varchar(2000), 
  co_applicant_race_name_4 varchar(2000), 
  co_applicant_race_name_5 varchar(2000), 
  co_applicant_sex int, 
  co_applicant_sex_name varchar(2000), 
  county_code int, 
  county_name varchar(2000), 
  denial_reason_1 int, 
  denial_reason_2 int, 
  denial_reason_3 int, 
  denial_reason_name_1 varchar(2000), 
  denial_reason_name_2 varchar(2000), 
  denial_reason_name_3 varchar(2000), 
  edit_status int, 
  edit_status_name varchar(2000), 
  hoepa_status int, 
  hoepa_status_name varchar(2000), 
  lien_status int, 
  lien_status_name varchar(2000), 
  loan_purpose int, 
  loan_purpose_name varchar(2000), 
  loan_type int, 
  loan_type_name varchar(2000), 
  msamd varchar(2000), 
  msamd_name varchar(2000), 
  owner_occupancy int, 
  owner_occupancy_name varchar(2000), 
  preapproval int, 
  preapproval_name varchar(2000), 
  property_type int, 
  property_type_name varchar(2000), 
  purchaser_type int, 
  purchaser_type_name varchar(2000), 
  respondent_id int, 
  sequence_number varchar(2000), 
  state_code int, 
  state_abbr varchar(2000), 
  state_name varchar(2000), 
  hud_median_family_income int, 
  loan_amount_000s int, 
  number_of_1_to_4_family_units int, 
  number_of_owner_occupied_units int, 
  minority_population decimal(5,2), 
  population int, 
  rate_spread decimal(5,2), 
  tract_to_msamd_income decimal(10,2))
PARTITIONED BY ( 
  as_of_year varchar(2000))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://datalake-poc-data/feeds/hmda/hmda_lar/clean/orc_snappy'
TABLE PROPERTIES (
  'orc.compress'='snappy');
  
ALTER TABLE hmda.hmda_lar
ADD PARTITION(as_of_year='2014')
LOCATION 's3://datalake-poc-data/feeds/hmda/hmda_lar/clean/orc_snappy/as_of_year=2014';

ALTER TABLE hmda.hmda_lar
ADD PARTITION(as_of_year='2015')
LOCATION 's3://datalake-poc-data/feeds/hmda/hmda_lar/clean/orc_snappy/as_of_year=2015';