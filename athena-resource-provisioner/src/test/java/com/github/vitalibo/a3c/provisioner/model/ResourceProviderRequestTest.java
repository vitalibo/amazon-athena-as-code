package com.github.vitalibo.a3c.provisioner.model;

import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ResourceProviderRequestTest {

    @Test
    public void testFromJson() {
        String resource = TestHelper.resourceAsJsonString("/CloudFormation/Request.json");

        ResourceProviderRequest actual = Jackson.fromJsonString(resource, ResourceProviderRequest.class);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getRequestType(), RequestType.Update);
        Assert.assertEquals(actual.getRequestId(), "unique id for this update request");
        Assert.assertEquals(actual.getResponseUrl(), "pre-signed-url-for-update-response");
        Assert.assertEquals(actual.getResourceType(), ResourceType.Unknown); // Unknown resource type "Custom::MyCustomResourceType"
        Assert.assertEquals(actual.getLogicalResourceId(), "name of resource in template");
        Assert.assertEquals(actual.getStackId(), "arn:aws:cloudformation:us-east-2:namespace:stack/stack-name/guid");
        Assert.assertEquals(actual.getPhysicalResourceId(), "custom resource provider-defined physical id");
        Assert.assertNotNull(actual.getResourceProperties());
        Properties resourceProperties = Jackson.convertValue(actual.getResourceProperties(), Properties.class);
        Assert.assertEquals(resourceProperties.getKey1(), "new-string");
        Assert.assertEquals(resourceProperties.getKey2(), Collections.singletonList("new-list"));
        Assert.assertEquals(resourceProperties.getKey3(), Collections.singletonMap("key4", "new-map"));
        Assert.assertNotNull(actual.getOldResourceProperties());
        Properties oldResourceProperties = Jackson.convertValue(actual.getOldResourceProperties(), Properties.class);
        Assert.assertEquals(oldResourceProperties.getKey1(), "string");
        Assert.assertEquals(oldResourceProperties.getKey2(), Collections.singletonList("list"));
        Assert.assertEquals(oldResourceProperties.getKey3(), Collections.singletonMap("key4", "map"));
    }

    @Data
    private static class Properties {

        private String key1;
        private List<String> key2;
        private Map<String, String> key3;

    }

}