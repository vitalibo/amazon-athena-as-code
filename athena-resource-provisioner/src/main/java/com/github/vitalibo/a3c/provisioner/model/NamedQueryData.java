package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class NamedQueryData extends ResourceData {

    @JsonProperty(value = "NamedQueryId")
    private String queryId;

}