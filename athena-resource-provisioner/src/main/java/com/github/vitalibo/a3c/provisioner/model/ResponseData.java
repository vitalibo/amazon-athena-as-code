package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class ResponseData {

    @JsonIgnore
    private String physicalResourceId;

}
