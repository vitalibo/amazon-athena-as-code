package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TableData extends ResourceData {

    @JsonProperty(value = "Name")
    private String name;

    public TableData withName(String name) {
        this.name = name;
        return this;
    }

}
