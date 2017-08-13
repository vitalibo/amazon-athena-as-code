package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class NamedQueryProperties extends ResourceProperties {

    @JsonProperty(value = "Name")
    private String name;

    @JsonProperty(value = "Description")
    private String description;

    @JsonProperty(value = "Database")
    private String database;

    @JsonProperty(value = "Query")
    private Query query;

    @Data
    public static class Query {

        @JsonProperty(value = "S3Bucket")
        private String s3Bucket;

        @JsonProperty(value = "S3Key")
        private String s3Key;

        @JsonProperty(value = "QueryString")
        private String queryString;

    }

}
