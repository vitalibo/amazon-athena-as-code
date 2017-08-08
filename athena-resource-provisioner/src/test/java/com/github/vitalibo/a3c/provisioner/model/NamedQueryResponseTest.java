package com.github.vitalibo.a3c.provisioner.model;

import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NamedQueryResponseTest {

    @Test
    public void testToJson() {
        NamedQueryResponse response = new NamedQueryResponse();
        response.setPhysicalResourceId("required vendor-defined physical id that is unique for that vendor");
        response.setQueryId("the unique identifier of the query");

        String actual = Jackson.toJsonString(response);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, TestHelper.resourceAsJsonString("/Athena/NamedQuery/Response.json"));
    }

}