package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class RequestProperties {

    @JsonProperty(value = "ServiceToken")
    private String serviceToken;

    @JsonProperty(value = "PhysicalResourceId")
    private String physicalResourceId;

}
