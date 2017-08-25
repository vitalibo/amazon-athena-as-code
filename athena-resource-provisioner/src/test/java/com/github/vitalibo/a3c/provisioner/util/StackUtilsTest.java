package com.github.vitalibo.a3c.provisioner.util;

import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StackUtilsTest {

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            {"amazon-athena-as-code-sample-SampleDB-"}, {"amazon-athena-as-code-test-sample-SampleDB-7FNJQI7BU3JP"},
            {"amazon-athena-as-code-sample-7FNJQI7BU3JP"}, {"SampleDB-7FNJQI7BU3JP"}, {""}
        };
    }

    @Test(dataProvider = "samples")
    public void testDoNotHasDefaultPhysicalResourceId(String physicalResourceId) {
        ResourceProviderRequest request = makeResourceProviderRequest(physicalResourceId);

        boolean actual = StackUtils.hasDefaultPhysicalResourceId(request);

        Assert.assertFalse(actual);
    }

    @Test
    public void testHasDefaultPhysicalResourceId() {
        ResourceProviderRequest request = makeResourceProviderRequest(
            "amazon-athena-as-code-sample-SampleDB-7FNJQI7BU3JP");

        boolean actual = StackUtils.hasDefaultPhysicalResourceId(request);

        Assert.assertTrue(actual);
    }

    @Test
    public void testMakeDefaultPhysicalResourceId() {
        ResourceProviderRequest request = makeResourceProviderRequest(null);

        String actual = StackUtils.makeDefaultPhysicalResourceId(request);

        Assert.assertNotNull(actual);
        Assert.assertTrue(StackUtils.hasDefaultPhysicalResourceId(
            makeResourceProviderRequest(actual)));
    }

    private static ResourceProviderRequest makeResourceProviderRequest(String physicalResourceId) {
        ResourceProviderRequest o = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/CloudFormation/Request.json"), ResourceProviderRequest.class);
        o.setStackId("arn:aws:cloudformation:eu-west-1:1234567890:stack/amazon-athena-as-code-sample/05a7d5f0-896f-11e7-a9e7-500c42421e36");
        o.setLogicalResourceId("SampleDB");
        o.setPhysicalResourceId(physicalResourceId);
        return o;
    }

}