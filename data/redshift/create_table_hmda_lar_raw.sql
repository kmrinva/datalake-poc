CREATE EXTERNAL TABLE hmda.hmda_lar_raw(
  action_taken varchar(2000), 
  action_taken_name varchar(2000), 
  agency_code varchar(2000), 
  agency_abbr varchar(2000), 
  agency_name varchar(2000), 
  applicant_ethnicity varchar(2000), 
  applicant_ethnicity_name varchar(2000),
  applicant_income_000s varchar(2000),
  applicant_race_1 varchar(2000),
  applicant_race_2 varchar(2000),
  applicant_race_3 varchar(2000),
  applicant_race_4 varchar(2000),
  applicant_race_5 varchar(2000),
  applicant_race_name_1 varchar(2000),
  applicant_race_name_2 varchar(2000),
  applicant_race_name_3 varchar(2000),
  applicant_race_name_4 varchar(2000),
  applicant_race_name_5 varchar(2000),
  applicant_sex varchar(2000),
  applicant_sex_name varchar(2000),
  application_date_indicator varchar(2000),
  as_of_year_placeholder varchar(2000),
  census_tract_number varchar(2000),
  co_applicant_ethnicity varchar(2000), 
  co_applicant_ethnicity_name varchar(2000), 
  co_applicant_race_1 varchar(2000), 
  co_applicant_race_2 varchar(2000), 
  co_applicant_race_3 varchar(2000), 
  co_applicant_race_4 varchar(2000), 
  co_applicant_race_5 varchar(2000), 
  co_applicant_race_name_1 varchar(2000), 
  co_applicant_race_name_2 varchar(2000), 
  co_applicant_race_name_3 varchar(2000), 
  co_applicant_race_name_4 varchar(2000), 
  co_applicant_race_name_5 varchar(2000), 
  co_applicant_sex varchar(2000), 
  co_applicant_sex_name varchar(2000), 
  county_code varchar(2000), 
  county_name varchar(2000), 
  denial_reason_1 varchar(2000), 
  denial_reason_2 varchar(2000), 
  denial_reason_3 varchar(2000), 
  denial_reason_name_1 varchar(2000), 
  denial_reason_name_2 varchar(2000), 
  denial_reason_name_3 varchar(2000), 
  edit_status varchar(2000), 
  edit_status_name varchar(2000), 
  hoepa_status varchar(2000), 
  hoepa_status_name varchar(2000), 
  lien_status varchar(2000), 
  lien_status_name varchar(2000), 
  loan_purpose varchar(2000), 
  loan_purpose_name varchar(2000), 
  loan_type varchar(2000), 
  loan_type_name varchar(2000), 
  msamd varchar(2000), 
  msamd_name varchar(2000), 
  owner_occupancy varchar(2000), 
  owner_occupancy_name varchar(2000), 
  preapproval varchar(2000), 
  preapproval_name varchar(2000), 
  property_type varchar(2000), 
  property_type_name varchar(2000), 
  purchaser_type varchar(2000), 
  purchaser_type_name varchar(2000), 
  respondent_id varchar(2000), 
  sequence_number varchar(2000), 
  state_code varchar(2000), 
  state_abbr varchar(2000), 
  state_name varchar(2000), 
  hud_median_family_income varchar(2000), 
  loan_amount_000s varchar(2000), 
  number_of_1_to_4_family_units varchar(2000), 
  number_of_owner_occupied_units varchar(2000), 
  minority_population varchar(2000), 
  population varchar(2000), 
  rate_spread varchar(2000), 
  tract_to_msamd_income varchar(2000))
PARTITIONED BY ( 
  as_of_year varchar(2000))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'quoteChar'='\"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://datalake-poc-data/feeds/hmda/hmda_lar/raw/csv'
TABLE PROPERTIES (
  'skip.header.line.count'='1'
);

ALTER TABLE hmda.hmda_lar_raw
ADD PARTITION(as_of_year='2014')
LOCATION 's3://datalake-poc-data/feeds/hmda/hmda_lar/raw/csv/as_of_year=2014';

ALTER TABLE hmda.hmda_lar_raw
ADD PARTITION(as_of_year='2015')
LOCATION 's3://datalake-poc-data/feeds/hmda/hmda_lar/raw/csv/as_of_year=2015';