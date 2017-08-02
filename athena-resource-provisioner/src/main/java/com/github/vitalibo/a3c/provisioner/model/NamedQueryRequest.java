package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class NamedQueryRequest extends RequestProperties {

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("Database")
    private String database;

    @JsonProperty("Query")
    private Query query;

    @Data
    public static class Query {

        @JsonProperty("S3Bucket")
        private String s3Bucket;

        @JsonProperty("S3Key")
        private String s3Key;

        @JsonProperty("QueryString")
        private String queryString;

    }

}
