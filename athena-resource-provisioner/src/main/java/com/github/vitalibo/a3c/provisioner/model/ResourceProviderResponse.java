package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ResourceProviderResponse {

    @JsonProperty(value = "Status")
    private ResponseStatus status;

    @JsonProperty(value = "Reason")
    private String reason;

    @JsonProperty(value = "PhysicalResourceId")
    private String physicalResourceId;

    @JsonProperty(value = "StackId")
    private String stackId;

    @JsonProperty(value = "RequestId")
    private String requestId;

    @JsonProperty(value = "LogicalResourceId")
    private String logicalResourceId;

    @JsonProperty(value = "Data")
    private ResponseData data;

    public ResourceProviderResponse withStatus(ResponseStatus status) {
        this.status = status;
        return this;
    }

    public ResourceProviderResponse withReason(String reason) {
        this.reason = reason;
        return this;
    }

    public ResourceProviderResponse withPhysicalResourceId(String physicalResourceId) {
        this.physicalResourceId = physicalResourceId;
        return this;
    }

    public ResourceProviderResponse withStackId(String stackId) {
        this.stackId = stackId;
        return this;
    }

    public ResourceProviderResponse withRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    public ResourceProviderResponse withLogicalResourceId(String logicalResourceId) {
        this.logicalResourceId = logicalResourceId;
        return this;
    }

    public ResourceProviderResponse withData(ResponseData data) {
        this.data = data;
        return this;
    }

}