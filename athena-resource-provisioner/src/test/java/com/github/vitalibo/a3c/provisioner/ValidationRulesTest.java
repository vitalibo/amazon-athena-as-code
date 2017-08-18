package com.github.vitalibo.a3c.provisioner;

import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.model.Property;
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

    private static String makeString(int length) {
        return IntStream.range(0, length)
            .mapToObj(o -> String.valueOf((char) o))
            .collect(Collectors.joining());
    }

}