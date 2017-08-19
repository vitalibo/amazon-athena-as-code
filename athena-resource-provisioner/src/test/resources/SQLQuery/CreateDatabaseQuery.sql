CREATE DATABASE `clickstreams`
COMMENT 'Site Foo clickstream data aggregates'
LOCATION 's3://myS3location/clickstreams'
WITH DBPROPERTIES (
    'creator'='Jane D.',
    'Dept.'='Marketing analytics'
)
;
