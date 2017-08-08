package com.github.vitalibo.a3c.provisioner.model;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ResourceTypeTest {

    @DataProvider
    public Object[][] samplesKnownResourceTypes() {
        return new Object[][]{
            {"Custom::AthenaNamedQuery"}
        };
    }

    @Test(dataProvider = "samplesKnownResourceTypes")
    public void testDeserializeKnownResourceType(String resourceType) {
        ResourceType actual = ResourceType.of(resourceType);

        Assert.assertNotNull(actual);
        Assert.assertNotEquals(actual, ResourceType.Unknown);
    }

    @DataProvider
    public Object[][] samplesUnknownResourceTypes() {
        return new Object[][]{
            {"Custom::UnknownResourceType"}, {"AWS::Athena::NamedQuery"}, {"AWS::Lambda::Function"}, {""}
        };
    }

    @Test(dataProvider = "samplesUnknownResourceTypes")
    public void testDeserializeUnknownResourceType(String resourceType) {
        ResourceType actual = ResourceType.of(resourceType);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, ResourceType.Unknown);
    }

}