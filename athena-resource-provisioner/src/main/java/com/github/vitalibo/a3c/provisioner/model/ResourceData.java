package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ResourceData {

    @JsonProperty(value = "PhysicalResourceId")
    private String physicalResourceId;

}
