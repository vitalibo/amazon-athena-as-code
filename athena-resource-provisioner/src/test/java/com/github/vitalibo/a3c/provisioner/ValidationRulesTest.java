package com.github.vitalibo.a3c.provisioner;

import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.model.Property;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ValidationRulesTest {

    @DataProvider
    public Object[][] samplesDatabase() {
        return new Object[][]{
            {null}, {""}, {makeString(33)}
        };
    }

    @Test(dataProvider = "samplesDatabase", expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Database.*")
    public void testVerifyDatabaseFailed(String database) {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDatabase(database);

        ValidationRules.verifyDatabase(properties);
    }

    @Test
    public void testVerifyDatabase() {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDatabase("database_name");

        ValidationRules.verifyDatabase(properties);
    }

    @DataProvider
    public Object[][] samplesDescription() {
        return new Object[][]{
            {null}, {""}, {"Some description"}
        };
    }

    @Test(dataProvider = "samplesDescription")
    public void testVerifyDescription(String description) {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDescription(description);

        ValidationRules.verifyDescription(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Description.*")
    public void testVerifyDescriptionFailed() {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDescription(makeString(1025));

        ValidationRules.verifyDescription(properties);
    }

    @Test
    public void testVerifyQueryString() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setQueryString("foo bar");
        properties.setQuery(query);

        ValidationRules.verifyQueryString(properties);
    }

    @Test
    public void testVerifyQueryStringOnS3() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setS3Bucket("s3 bucket");
        query.setS3Key("s3 key");
        properties.setQuery(query);

        ValidationRules.verifyQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*QueryString.*")
    public void testVerifyQueryStringIsNull() {
        NamedQueryProperties properties = new NamedQueryProperties();

        ValidationRules.verifyQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*QueryString.*")
    public void testVerifyQueryStringFailed() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setQueryString(makeString(262145));

        ValidationRules.verifyQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*S3Bucket.*")
    public void testVerifyQueryStringMissingS3Key() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setS3Bucket("s3 bucket");
        properties.setQuery(query);

        ValidationRules.verifyQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*S3Key.*")
    public void testVerifyQueryStringMissingS3Bucket() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setS3Key("s3 key");
        properties.setQuery(query);

        ValidationRules.verifyQueryString(properties);
    }

    @DataProvider
    public Object[][] samplesName() {
        return new Object[][]{
            {null}, {""}, {makeString(33)}
        };
    }

    @Test(dataProvider = "samplesName", expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Name.*")
    public void testVerifyDatabaseNameFailed(String name) {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setName(name);

        ValidationRules.verifyName(properties);
    }

    @Test
    public void testVerifyDatabaseName() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setName("database_name");

        ValidationRules.verifyName(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Location.*")
    public void testVerifyDatabaseLocationFailed() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setLocation("s3_bucket");

        ValidationRules.verifyLocation(properties);
    }

    @Test
    public void testVerifyDatabaseLocation() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setLocation("s3://s3_bucket/");

        ValidationRules.verifyLocation(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Comment.*")
    public void testVerifyCommentFailed() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setComment(makeString(1025));

        ValidationRules.verifyComment(properties);
    }

    @Test
    public void testVerifyComment() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setComment("some comment");

        ValidationRules.verifyComment(properties);
    }

    @Test
    public void testVerifyProperties() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setProperties(Collections.singletonList(
            new Property("foo", "bar")));

        ValidationRules.verifyProperties(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Properties.0..Name.*")
    public void testVerifyPropertiesMissingName() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setProperties(Collections.singletonList(
            new Property(null, "bar")));

        ValidationRules.verifyProperties(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Properties.0..Value.*")
    public void testVerifyPropertiesMissingValue() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setProperties(Collections.singletonList(
            new Property("foo", null)));

        ValidationRules.verifyProperties(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectName() {
        return new Object[][]{
            {null}, {""}, {makeString(129)}
        };
    }

    @Test(dataProvider = "samplesIncorrectName", expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Name.*")
    public void testVerifyNameFailed(String name) {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setName(name);

        ValidationRules.verifyName(tableProperties);
    }

    @Test
    public void testVerifyName() {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setName("table_name");

        ValidationRules.verifyName(tableProperties);
    }

    @Test(dataProvider = "samplesDatabase", expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Database.*")
    public void testVerifyTableDatabase(String database) {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setDatabaseName(database);

        ValidationRules.verifyDatabase(tableProperties);
    }

    @Test
    public void testVerifyTableComment() {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setComment("some_comment");

        ValidationRules.verifyComment(tableProperties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Comment.*")
    public void testVerifyTableCommentFailed() {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setComment(makeString(1025));

        ValidationRules.verifyComment(tableProperties);
    }

    @DataProvider
    public Object[][] samplesStoredAs() {
        return new Object[][]{
            {null}, {""}, {"SEQUENCEFILE"}, {"TEXTFILE"}, {"RCFILE"}, {"ORC"}, {"PARQUET"}, {"AVRO"},
            {"INPUTFORMAT `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat` OUTPUTFORMAT `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`"}
        };
    }

    @Test(dataProvider = "samplesStoredAs")
    public void testVerifyStoredAs(String storedAs) {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setStoredAs(storedAs);

        ValidationRules.verifyStoredAs(tableProperties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*StoredAs.*")
    public void testVerifyStoredAsFailed() {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setStoredAs("foo");

        ValidationRules.verifyStoredAs(tableProperties);
    }

    @Test
    public void testVerifyTableLocation() {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setLocation("s3://sample.s3.bucket/folder/");

        ValidationRules.verifyLocation(tableProperties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Location.*")
    public void testVerifyTableLocationFail() {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setLocation("sample.s3.bucket/folder/");

        ValidationRules.verifyLocation(tableProperties);
    }

    @Test
    public void testVerifyTableProperties() {
        TableProperties properties = new TableProperties();
        properties.setProperties(Collections.singletonList(
            new Property("foo", "bar")));

        ValidationRules.verifyProperties(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Properties.0..Name.*")
    public void testVerifyTablePropertiesFails() {
        TableProperties properties = new TableProperties();
        properties.setProperties(Collections.singletonList(
            new Property(null, "bar")));

        ValidationRules.verifyProperties(properties);
    }

    @DataProvider
    public Object[][] samplesDataType() {
        return new Object[][]{
            {"TINYINT"}, {"SMALLINT"}, {"INT"}, {"BIGINT"}, {"BOOLEAN"}, {"DOUBLE"},
            {"STRING"}, {"BINARY"}, {"TIMESTAMP"}, {"DECIMAL"}, {"DATE"}, {"VARCHAR"},
            {"ARRAY<STRING>"}, {"MAP<STRING,STRING>"}, {"STRUCT<Name:STRING>"}
        };
    }

    @Test(dataProvider = "samplesDataType")
    public void testVerifySchema(String type) {
        TableProperties properties = new TableProperties();
        TableProperties.Column column = new TableProperties.Column();
        column.setName("foo");
        column.setType(type);
        properties.setSchema(Collections.singletonList(column));

        ValidationRules.verifySchema(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Schema.*Name.*")
    public void testVerifySchemaMissingName() {
        TableProperties properties = new TableProperties();
        TableProperties.Column column = new TableProperties.Column();
        column.setType("STRING");
        column.setComment("bar");
        properties.setSchema(Collections.singletonList(column));

        ValidationRules.verifySchema(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Schema.*Type.*")
    public void testVerifySchemaMissingType() {
        TableProperties properties = new TableProperties();
        TableProperties.Column column = new TableProperties.Column();
        column.setName("foo");
        column.setComment("bar");
        properties.setSchema(Collections.singletonList(column));

        ValidationRules.verifySchema(properties);
    }

    @Test
    public void testVerifyPartition() {
        TableProperties properties = new TableProperties();
        TableProperties.Column column = new TableProperties.Column();
        column.setName("foo");
        column.setType("STRING");
        column.setComment("some comment");
        properties.setPartition(Collections.singletonList(column));

        ValidationRules.verifyPartition(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Partition.*Type.*")
    public void testVerifyPartitionFailed() {
        TableProperties properties = new TableProperties();
        TableProperties.Column column = new TableProperties.Column();
        column.setName("foo");
        column.setComment("some comment");
        properties.setPartition(Collections.singletonList(column));

        ValidationRules.verifyPartition(properties);
    }

    private static String makeString(int length) {
        return IntStream.range(0, length)
            .mapToObj(o -> String.valueOf((char) o))
            .collect(Collectors.joining());
    }

}