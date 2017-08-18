package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ResourceData {

    @JsonProperty(value = "PhysicalResourceId")
    private String physicalResourceId;

    @SuppressWarnings("unchecked")
    public <T extends ResourceData> T withPhysicalResourceId(String physicalResourceId) {
        this.physicalResourceId = physicalResourceId;
        return (T) this;
    }

}
