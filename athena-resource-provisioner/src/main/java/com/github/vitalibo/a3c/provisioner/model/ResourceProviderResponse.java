package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResourceProviderResponse {

    @JsonProperty(value = "Status")
    private Status status;

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
    private ResourceData data;

}