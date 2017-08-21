package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class TableProperties extends ResourceProperties {

    @JsonProperty(value = "Name")
    private String name;

    @JsonProperty(value = "Database")
    private String databaseName;

    @JsonProperty(value = "Schema")
    private List<Column> schema;

    @JsonProperty(value = "Comment")
    private String comment;

    @JsonProperty(value = "Partition")
    private List<Column> partition;

    @JsonProperty(value = "RowFormat")
    private RowFormat rowFormat;

    @JsonProperty(value = "StoredAs")
    private String storedAs;

    @JsonProperty(value = "Location")
    private String location;

    @JsonProperty(value = "Properties")
    private List<Property> properties;

    @Data
    public static class Column {

        @JsonProperty(value = "Name")
        private String name;

        @JsonProperty(value = "Type")
        private String type;

        @JsonProperty(value = "Comment")
        private String comment;

    }

    @Data
    public static class RowFormat {

        @JsonProperty(value = "SerDe")
        private String serDe;

        @JsonProperty(value = "Properties")
        private List<Property> properties;

    }

}