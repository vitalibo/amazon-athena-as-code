package com.github.vitalibo.a3c.provisioner;

import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.model.Property;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Random;
import java.util.stream.Collectors;

public class ValidationRulesTest {

    @DataProvider
    public Object[][] samplesIncorrectDatabase() {
        return new Object[][]{
            {null}, {""}, {string(33)}
        };
    }

    @Test(dataProvider = "samplesIncorrectDatabase",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Database.*")
    public void testVerifyNamedQueryDatabaseFail(String o) {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDatabase(o);

        ValidationRules.verifyNamedQueryDatabase(properties);
    }

    @Test
    public void testVerifyNamedQueryDatabase() {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDatabase("database_name");

        ValidationRules.verifyNamedQueryDatabase(properties);
    }

    @DataProvider
    public Object[][] samplesDescription() {
        return new Object[][]{
            {null}, {""}, {string(1024)}
        };
    }

    @Test(dataProvider = "samplesDescription")
    public void testVerifyNamedQueryDescription(String o) {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDescription(o);

        ValidationRules.verifyNamedQueryDescription(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Description.*")
    public void testVerifyNamedQueryDescriptionFail() {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setDescription(string(1025));

        ValidationRules.verifyNamedQueryDescription(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectName() {
        return new Object[][]{
            {null}, {""}, {string(129)}
        };
    }

    @Test(dataProvider = "samplesIncorrectName",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Name.*")
    public void testVerifyNamedQueryNameFail(String o) {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setName(o);

        ValidationRules.verifyNamedQueryName(properties);
    }

    @Test
    public void testVerifyNamedQueryName() {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setName("query_name");

        ValidationRules.verifyNamedQueryName(properties);
    }

    @Test
    public void verifyNamedQueryQueryString() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setQueryString("foo bar");
        properties.setQuery(query);

        ValidationRules.verifyNamedQueryQueryString(properties);
    }

    @Test
    public void verifyNamedQueryQueryS3Strategy() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setS3Bucket("foo");
        query.setS3Key("bar");
        properties.setQuery(query);

        ValidationRules.verifyNamedQueryQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*QueryString.*")
    public void verifyNamedQueryQueryStringIsNull() {
        NamedQueryProperties properties = new NamedQueryProperties();
        properties.setQuery(null);

        ValidationRules.verifyNamedQueryQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*S3Bucket.*")
    public void verifyNamedQueryQueryStringMissingS3Bucket() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setS3Key("bar");
        properties.setQuery(query);

        ValidationRules.verifyNamedQueryQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*S3Key.*")
    public void verifyNamedQueryQueryStringMissingS3Key() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setS3Bucket("foo");
        properties.setQuery(query);

        ValidationRules.verifyNamedQueryQueryString(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class, expectedExceptionsMessageRegExp = ".*Query.*")
    public void verifyNamedQueryQueryStringFail() {
        NamedQueryProperties properties = new NamedQueryProperties();
        NamedQueryProperties.Query query = new NamedQueryProperties.Query();
        query.setS3Bucket("foo");
        query.setQueryString("query_string");
        properties.setQuery(query);

        ValidationRules.verifyNamedQueryQueryString(properties);
    }

    @Test(dataProvider = "samplesIncorrectDatabase",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Name.*")
    public void testVerifyDatabaseNameFail(String o) {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setName(o);

        ValidationRules.verifyDatabaseName(properties);
    }

    @Test
    public void testVerifyDatabaseName() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setName("database_name");

        ValidationRules.verifyDatabaseName(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectLocation() {
        return new Object[][]{
            {"s3://path_to_bucket"}, {"s3://path_to_bucket/*"},
            {"s3://path_to-bucket/mydatafile.dat"}
        };
    }

    @Test(dataProvider = "samplesIncorrectLocation",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Location.*")
    public void testVerifyDatabaseLocationFail(String o) {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setLocation(o);

        ValidationRules.verifyDatabaseLocation(properties);
    }

    @DataProvider
    public Object[][] samplesLocation() {
        return new Object[][]{
            {null}, {""}, {"s3://mybucket/myfolder/"}
        };
    }

    @Test(dataProvider = "samplesLocation")
    public void testVerifyDatabaseLocation(String o) {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setLocation(o);

        ValidationRules.verifyDatabaseLocation(properties);
    }

    @DataProvider
    public Object[][] samplesComment() {
        return new Object[][]{
            {null}, {""}, {string(1024)}
        };
    }

    @Test(dataProvider = "samplesComment")
    public void testVerifyDatabaseComment(String o) {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setComment(o);

        ValidationRules.verifyDatabaseComment(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Comment.*")
    public void testVerifyDatabaseCommentFail() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setComment(string(1025));

        ValidationRules.verifyDatabaseComment(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectProperties() {
        return new Object[][]{
            {new Property(null, null)},
            {new Property("foo", null)},
            {new Property(null, "bar")},
        };
    }

    @Test(dataProvider = "samplesIncorrectProperties",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Properties.*")
    public void testVerifyDatabasePropertiesFail(Property o) {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setProperties(Collections.singletonList(o));

        ValidationRules.verifyDatabaseProperties(properties);
    }

    @Test
    public void testVerifyDatabaseProperties() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setProperties(Collections.singletonList(
            new Property("foo", "bar")));

        ValidationRules.verifyDatabaseProperties(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectTableName() {
        return new Object[][]{
            {null}, {""}, {string(129)}, {"table-name"}
        };
    }

    @Test(dataProvider = "samplesIncorrectTableName",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Name.*")
    public void testVerifyTableNameFail(String o) {
        TableProperties properties = new TableProperties();
        properties.setName(o);

        ValidationRules.verifyTableName(properties);
    }

    @Test
    public void testVerifyTableName() {
        TableProperties properties = new TableProperties();
        properties.setName("table_name");

        ValidationRules.verifyTableName(properties);
    }

    @Test(dataProvider = "samplesIncorrectDatabase",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Database.*")
    public void testVerifyTableDatabaseFail(String o) {
        TableProperties properties = new TableProperties();
        properties.setDatabaseName(o);

        ValidationRules.verifyTableDatabase(properties);
    }

    @Test
    public void testVerifyTableDatabase() {
        TableProperties properties = new TableProperties();
        properties.setDatabaseName("database_name");

        ValidationRules.verifyTableDatabase(properties);
    }

    @Test(dataProvider = "samplesComment")
    public void testVerifyTableComment(String o) {
        TableProperties properties = new TableProperties();
        properties.setComment(o);

        ValidationRules.verifyTableComment(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Comment.*")
    public void testVerifyTableCommentFail() {
        TableProperties properties = new TableProperties();
        properties.setComment(string(1025));

        ValidationRules.verifyTableComment(properties);
    }

    @DataProvider
    public Object[][] samplesStoredAs() {
        return new Object[][]{
            {null}, {""}, {"SEQUENCEFILE"}, {"TEXTFILE"}, {"RCFILE"}, {"ORC"}, {"PARQUET"}, {"AVRO"},
            {"INPUTFORMAT `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat` OUTPUTFORMAT `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`"}
        };
    }

    @Test(dataProvider = "samplesStoredAs")
    public void testVerifyTableStoredAs(String o) {
        TableProperties properties = new TableProperties();
        properties.setStoredAs(o);

        ValidationRules.verifyTableStoredAs(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*StoredAs.*")
    public void testVerifyTableStoredAsFail() {
        TableProperties properties = new TableProperties();
        properties.setStoredAs("foo");

        ValidationRules.verifyTableStoredAs(properties);
    }

    @Test(dataProvider = "samplesIncorrectLocation",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Location.*")
    public void testVerifyTableLocationFail(String o) {
        TableProperties properties = new TableProperties();
        properties.setLocation(o);

        ValidationRules.verifyTableLocation(properties);
    }

    @Test(dataProvider = "samplesLocation")
    public void testVerifyTableLocation(String o) {
        TableProperties properties = new TableProperties();
        properties.setLocation(o);

        ValidationRules.verifyTableLocation(properties);
    }

    @DataProvider
    public Object[][] samplesColumn() {
        return new Object[][]{
            {makeColumn("foo", "STRING", "Some comment")},
            {makeColumn("foo", "ARRAY<STRING>", null)}
        };
    }

    @DataProvider
    public Object[][] samplesIncorrectColumn() {
        return new Object[][]{
            {makeColumn(null, "ARRAY<STRING>", null)},
            {makeColumn("foo", null, "Some comment")},
            {makeColumn(null, null, null)},
        };
    }

    @Test(dataProvider = "samplesColumn")
    public void testVerifyTableSchema(TableProperties.Column o) {
        TableProperties properties = new TableProperties();
        properties.setSchema(Collections.singletonList(o));

        ValidationRules.verifyTableSchema(properties);
    }

    @Test(dataProvider = "samplesIncorrectColumn",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Schema.*")
    public void testVerifyTableSchemaFail(TableProperties.Column o) {
        TableProperties properties = new TableProperties();
        properties.setSchema(Collections.singletonList(o));

        ValidationRules.verifyTableSchema(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Schema.*")
    public void testVerifyTableSchemaIsEmpty() {
        TableProperties properties = new TableProperties();
        properties.setSchema(null);

        ValidationRules.verifyTableSchema(properties);
    }

    @DataProvider
    public Object[][] samplesColumnType() {
        return new Object[][]{
            {"TINYINT"}, {"SMALLINT"}, {"INT"}, {"BIGINT"}, {"BOOLEAN"}, {"DOUBLE"},
            {"STRING"}, {"BINARY"}, {"TIMESTAMP"}, {"DECIMAL"}, {"DATE"}, {"VARCHAR"},
            {"ARRAY<STRING>"}, {"MAP<STRING,STRING>"}, {"STRUCT<Name:STRING>"}
        };
    }

    @Test(dataProvider = "samplesColumnType")
    public void testVerifyColumnType(String o) {
        TableProperties properties = new TableProperties();
        properties.setSchema(Collections.singletonList(
            makeColumn("foo", o, null)));

        ValidationRules.verifyTableSchema(properties);
    }

    @Test(dataProvider = "samplesColumn")
    public void testVerifyTablePartition(TableProperties.Column o) {
        TableProperties properties = new TableProperties();
        properties.setPartition(Collections.singletonList(o));

        ValidationRules.verifyTablePartition(properties);
    }

    @Test(dataProvider = "samplesIncorrectColumn",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Partition.*")
    public void testVerifyTablePartitionFail(TableProperties.Column o) {
        TableProperties properties = new TableProperties();
        properties.setPartition(Collections.singletonList(o));

        ValidationRules.verifyTablePartition(properties);
    }

    @Test
    public void testVerifyTablePartitionIsEmpty() {
        TableProperties properties = new TableProperties();
        properties.setPartition(null);

        ValidationRules.verifyTablePartition(properties);
    }

    @Test(dataProvider = "samplesIncorrectProperties",
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Properties.*")
    public void testVerifyTablePropertiesFail(Property o) {
        TableProperties properties = new TableProperties();
        properties.setProperties(Collections.singletonList(o));

        ValidationRules.verifyTableProperties(properties);
    }

    @Test
    public void testVerifyTableProperties() {
        TableProperties properties = new TableProperties();
        properties.setProperties(Collections.singletonList(
            new Property("foo", "bar")));

        ValidationRules.verifyTableProperties(properties);
    }

    @Test
    public void testVerifyTableRowFormatIsNull() {
        TableProperties properties = new TableProperties();
        properties.setRowFormat(null);

        ValidationRules.verifyTableRowFormat(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SerDe.*")
    public void testVerifyTableRowFormatSerDeIsNull() {
        TableProperties properties = new TableProperties();
        properties.setRowFormat(
            makeRowFormat(null, new Property("foo", "bar")));

        ValidationRules.verifyTableRowFormat(properties);
    }

    @Test(expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Properties.*")
    public void testVerifyTableRowFormatSerDeIncorrectProperty() {
        TableProperties properties = new TableProperties();
        properties.setRowFormat(
            makeRowFormat("foo", new Property(null, "bar")));

        ValidationRules.verifyTableRowFormat(properties);
    }

    @Test
    public void testVerifyTableRowFormat() {
        TableProperties properties = new TableProperties();
        properties.setRowFormat(
            makeRowFormat("foo", new Property("foo", "bar")));

        ValidationRules.verifyTableRowFormat(properties);
    }

    private static TableProperties.RowFormat makeRowFormat(String serDe, Property property) {
        TableProperties.RowFormat o = new TableProperties.RowFormat();
        o.setSerDe(serDe);
        o.setProperties(Collections.singletonList(property));
        return o;
    }

    private static TableProperties.Column makeColumn(String name, String type, String comment) {
        TableProperties.Column o = new TableProperties.Column();
        o.setName(name);
        o.setType(type);
        o.setComment(comment);
        return o;
    }

    private static String string(int length) {
        return new Random().ints(length)
            .mapToObj(o -> String.valueOf((char) o))
            .collect(Collectors.joining());
    }

}