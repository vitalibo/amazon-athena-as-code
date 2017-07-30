package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ResourceProviderRequest {

    @JsonProperty(value = "RequestType")
    private RequestType requestType;

    @JsonProperty(value = "ResponseURL")
    private String responseUrl;

    @JsonProperty(value = "StackId")
    private String stackId;

    @JsonProperty(value = "RequestId")
    private String requestId;

    @JsonProperty(value = "ResourceType")
    private ResourceType resourceType;

    @JsonProperty(value = "LogicalResourceId")
    private String logicalResourceId;

    @JsonProperty(value = "PhysicalResourceId")
    private String physicalResourceId;

    @JsonProperty(value = "ResourceProperties")
    private Object resourceProperties;

    @JsonProperty(value = "OldResourceProperties")
    private Object oldResourceProperties;

}