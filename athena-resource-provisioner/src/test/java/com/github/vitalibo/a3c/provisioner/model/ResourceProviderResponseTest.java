package com.github.vitalibo.a3c.provisioner.model;

import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import lombok.Builder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ResourceProviderResponseTest {

    @Test
    public void testToJson() {
        ResourceProviderResponse response = ResourceProviderResponse.builder()
            .status(Status.SUCCESS)
            .logicalResourceId("name of resource in template (copied from request)")
            .requestId("unique id for this create request (copied from request)")
            .reason("sample of reason string")
            .stackId("arn:aws:cloudformation:us-east-2:namespace:stack/stack-name/guid (copied from request)")
            .physicalResourceId("required vendor-defined physical id that is unique for that vendor")
            .data(Data.builder()
                .keyThatCanBeUsedInGetAtt1("data for key 1")
                .keyThatCanBeUsedInGetAtt2("data for key 2")
                .build())
            .build();

        String actual = Jackson.toJsonString(response);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, TestHelper.resourceAsJsonString("/CloudFormation/Response.json"));
    }

    @Builder
    @lombok.Data
    private static class Data extends ResourceData {

        private String keyThatCanBeUsedInGetAtt1;
        private String keyThatCanBeUsedInGetAtt2;

    }

}