package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class NamedQueryResponse extends ResourceData {

    @JsonProperty("NamedQueryId")
    private String queryId;

}