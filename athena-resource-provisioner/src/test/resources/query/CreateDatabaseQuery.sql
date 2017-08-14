CREATE DATABASE the database name
LOCATION 's3://specifies the location where database files and metastore will be stored/'
WITH DBPROPERTIES (
    'custom metadata properties key for the database definition'='custom metadata properties value for the database definition',
    'key #2'='value #2'
);

