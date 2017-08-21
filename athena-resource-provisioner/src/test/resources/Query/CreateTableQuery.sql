CREATE EXTERNAL TABLE `MyTableName` (
    `Column1` STRING  COMMENT 'Comment #1',
    `Column2` ARRAY<STRING> ,
    `Column3` TIMESTAMP  COMMENT 'Comment #2'
)
COMMENT 'This is the Table Comment.'
PARTITIONED BY (
    `Partition1` TIMESTAMP ,
    `Partition2` STRING  COMMENT 'Comment #3'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format'='1'
)
STORED AS TEXTFILE
LOCATION 'aaa'
TBLPROPERTIES (
    'has_encrypted_data'='false',
    'Key1'='Value2'
);

