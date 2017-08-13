package com.github.vitalibo.a3c.provisioner.model;

import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NamedQueryPropertiesTest {

    @Test
    public void testFromJson() {
        String resource = TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json");

        NamedQueryProperties actual = Jackson.fromJsonString(resource, NamedQueryProperties.class);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getServiceToken(), "arn:aws:lambda:eu-west-1:0123456789:function:athena-resource-provisioner");
        Assert.assertEquals(actual.getName(), "the plain-language name of the query");
        Assert.assertEquals(actual.getDescription(), "a brief description of the query");
        Assert.assertEquals(actual.getDatabase(), "the database to which the query belongs");
        Assert.assertNotNull(actual.getQuery());
        NamedQueryProperties.Query query = actual.getQuery();
        Assert.assertEquals(query.getS3Bucket(), "the name of the S3 bucket");
        Assert.assertEquals(query.getS3Key(), "the location and name");
        Assert.assertEquals(query.getQueryString(), "the SQL query statements that comprise the query");
    }

}