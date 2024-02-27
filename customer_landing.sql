CREATE EXTERNAL TABLE `customer_landing`(
  `serialnumber` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` timestamp COMMENT 'from deserializer', 
  `birthday` date COMMENT 'from deserializer', 
  `registrationdate` timestamp COMMENT 'from deserializer', 
  `sharewithresearchasofdate` timestamp COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `lastupdatedate` timestamp COMMENT 'from deserializer', 
  `phone` bigint COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` timestamp COMMENT 'from deserializer')
COMMENT 'the customer landing table'
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-project-3/customer/landing'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1708852399')