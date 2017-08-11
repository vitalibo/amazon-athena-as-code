package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class ExternalTableRequest extends ResourceProperties {

    @JsonProperty(value = "Name")
    private String name;

    @JsonProperty(value = "Properties")
    private List<Property> properties;

}
