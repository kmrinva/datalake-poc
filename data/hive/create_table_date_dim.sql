
CREATE EXTERNAL TABLE dim_date(
   datekey  int
,  fulldate  varchar(10)
,  datename varchar(11)
,  dayofweek  int 
,  daynameofweek varchar(10)
,  dayofmonth int
,  dayofyear  int
,  weekdayweekend varchar(10)
,  weekofyear int 
,  monthname varchar(10)
,  monthofyear int
,  islastdayofmonth varchar(1)
,  calnedarquarter int
,  calendaryear int
,  calendaryearmonth varchar(10)
,  calendaryearqtr varchar(10) 
,  fiscalmonthofyear int
,  fiscalquarter int 
,  fiscalyear int
,  fiscalyearmonth varchar(10)
,  fiscalyearqtr varchar(10)
,  insertauditkey int 
,  updateauditkey int
)
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
  's3://datalake-poc-data/curated/utility_tables/date_dimension/csv'
TBLPROPERTIES (
  'serialization.null.format'='', 'skip.header.line.count'='1')