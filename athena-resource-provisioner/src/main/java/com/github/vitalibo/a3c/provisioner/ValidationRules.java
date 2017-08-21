package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.model.Property;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ValidationRules {

    private static final Set<Pattern> KNOWN_STORED_AS_VALUES = Stream
        .of(
            "SEQUENCEFILE", "TEXTFILE", "RCFILE", "ORC", "PARQUET",
            "AVRO", "INPUTFORMAT `.+` OUTPUTFORMAT `.+`")
        .map(Pattern::compile)
        .collect(Collectors.toSet());

    private static final Set<Pattern> KNOWN_COLUMN_TYPES = Stream
        .of(
            "TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "DOUBLE",
            "STRING", "BINARY", "TIMESTAMP", "DECIMAL.*", "DATE", "VARCHAR",
            "ARRAY.+", "MAP.+", "STRUCT.+")
        .map(Pattern::compile)
        .collect(Collectors.toSet());

    private ValidationRules() {
        super();
    }

    static void verifyNamedQueryDatabase(NamedQueryProperties o) {
        StringValidator
            .of("Database", o.getDatabase())
            .verifyRequired()
            .verifyLengthConstraints(1, 32);
    }

    static void verifyNamedQueryDescription(NamedQueryProperties o) {
        StringValidator
            .of("Description", o.getDescription())
            .verifyLengthConstraints(1, 1024);
    }

    static void verifyNamedQueryName(NamedQueryProperties o) {
        StringValidator
            .of("Name", o.getName())
            .verifyRequired()
            .verifyLengthConstraints(1, 128);
    }

    static void verifyNamedQueryQueryString(NamedQueryProperties o) {
        if (o.getQuery() == null) {
            throw new AthenaProvisionException(
                "Required property \"QueryString\" cannot be null or empty.");
        }

        NamedQueryProperties.Query q = o.getQuery();
        if (StringUtils.isNullOrEmpty(q.getQueryString())) {
            if (StringUtils.isNullOrEmpty(q.getS3Bucket()) || StringUtils.isNullOrEmpty(q.getS3Key())) {
                throw new AthenaProvisionException(
                    "The properties \"S3Bucket\" and \"S3Key\" must be present.");
            }

            return;
        } else {
            if (!StringUtils.isNullOrEmpty(q.getS3Bucket()) || !StringUtils.isNullOrEmpty(q.getS3Key())) {
                throw new AthenaProvisionException(
                    "Use one following of strategy for \"Query\" property: S3 or QueryString");
            }
        }

        StringValidator
            .of("QueryString", q.getQueryString())
            .verifyLengthConstraints(1, 262144);
    }

    static void verifyDatabaseName(DatabaseProperties o) {
        StringValidator
            .of("Name", o.getName())
            .verifyRequired()
            .verifyLengthConstraints(1, 32);
    }

    static void verifyDatabaseLocation(DatabaseProperties o) {
        StringValidator
            .of("Location", o.getLocation())
            .verifyMatchRegex("s3://.*/");
    }

    static void verifyDatabaseComment(DatabaseProperties o) {
        StringValidator
            .of("Comment", o.getComment())
            .verifyLengthConstraints(1, 1024);
    }

    static void verifyDatabaseProperties(DatabaseProperties o) {
        PropertyValidator
            .of("Properties", o.getProperties())
            .verifyProperties();
    }

    static void verifyTableName(TableProperties o) {
        StringValidator
            .of("Name", o.getName())
            .verifyRequired()
            .verifyLengthConstraints(1, 128)
            .verifyMatchRegex("[0-9A-Za-z_]+");
    }

    static void verifyTableDatabase(TableProperties o) {
        StringValidator
            .of("Database", o.getDatabaseName())
            .verifyRequired()
            .verifyLengthConstraints(1, 32);
    }

    static void verifyTableComment(TableProperties o) {
        StringValidator
            .of("Comment", o.getComment())
            .verifyLengthConstraints(1, 1024);
    }

    static void verifyTableStoredAs(TableProperties o) {
        StringValidator
            .of("StoredAs", o.getStoredAs())
            .verifyAnyMatch(KNOWN_STORED_AS_VALUES);
    }

    static void verifyTableLocation(TableProperties o) {
        StringValidator
            .of("Location", o.getLocation())
            .verifyMatchRegex("s3://.*/");
    }

    static void verifyTableProperties(TableProperties o) {
        PropertyValidator
            .of("Properties", o.getProperties())
            .verifyProperties();
    }

    static void verifyTableSchema(TableProperties o) {
        ColumnValidator
            .of("Schema", o.getSchema())
            .verifyRequired()
            .verifyColumns();
    }

    static void verifyTablePartition(TableProperties o) {
        ColumnValidator
            .of("Partition", o.getPartition())
            .verifyColumns();
    }

    static void verifyTableRowFormat(TableProperties o) {
        if (o.getRowFormat() == null) {
            return;
        }

        StringValidator
            .of("SerDe", o.getRowFormat().getSerDe())
            .verifyRequired();

        PropertyValidator
            .of("Properties", o.getRowFormat().getProperties())
            .verifyProperties();
    }

    @AllArgsConstructor(staticName = "of")
    static class StringValidator {

        private final String name;
        private final String value;

        StringValidator verifyRequired() {
            if (StringUtils.isNullOrEmpty(value)) {
                throw new AthenaProvisionException(String.format(
                    "Required property \"%s\" cannot be null or empty.", name));
            }

            return this;
        }

        StringValidator verifyLengthConstraints(int min, int max) {
            if (StringUtils.isNullOrEmpty(value)) {
                return this;
            }

            if (value.length() < min || value.length() > max) {
                throw new AthenaProvisionException(String.format(
                    "The \"%s\" property has length constraints: Minimum length of %d. Maximum length of %d.", name, min, max));
            }

            return this;
        }

        StringValidator verifyMatchRegex(String regex) {
            if (StringUtils.isNullOrEmpty(value)) {
                return this;
            }

            if (!value.matches(regex)) {
                throw new AthenaProvisionException(String.format(
                    "The \"%s\" property must match to regex \"%s\"", name, regex));
            }

            return this;
        }

        StringValidator verifyAnyMatch(Set<Pattern> patterns) {
            if (StringUtils.isNullOrEmpty(value)) {
                return this;
            }

            if (patterns.stream().map(o -> o.matcher(value)).noneMatch(Matcher::matches)) {
                throw new AthenaProvisionException(String.format(
                    "The value \"%s\" is not allowed for property \"%s\".", value, name));
            }

            return this;
        }

    }

    @AllArgsConstructor(staticName = "of")
    static class PropertyValidator {

        private final String name;
        private final List<Property> properties;

        PropertyValidator verifyProperties() {
            if (properties == null) {
                return this;
            }

            for (int i = 0; i < properties.size(); i++) {
                verifyProperty(String.format("%s[%d]", name, i), properties.get(i));
            }

            return this;
        }

        private static void verifyProperty(String name, Property property) {
            StringValidator
                .of(name + ".Name", property.getName())
                .verifyRequired();

            StringValidator
                .of(name + ".Value", property.getValue())
                .verifyRequired();
        }

    }

    @AllArgsConstructor(staticName = "of")
    static class ColumnValidator {

        private final String name;
        private final List<TableProperties.Column> columns;

        ColumnValidator verifyRequired() {
            if (columns == null || columns.isEmpty()) {
                throw new AthenaProvisionException(String.format(
                    "Required property \"%s\" cannot be null or empty.", name));
            }

            return this;
        }

        ColumnValidator verifyColumns() {
            if (columns == null) {
                return this;
            }

            for (int i = 0; i < columns.size(); i++) {
                verifyColumn(String.format("%s[%d]", name, i), columns.get(i));
            }

            return this;
        }

        private static void verifyColumn(String name, TableProperties.Column column) {
            StringValidator
                .of(name + ".Name", column.getName())
                .verifyRequired();

            StringValidator
                .of(name + ".Type", column.getType())
                .verifyRequired()
                .verifyAnyMatch(KNOWN_COLUMN_TYPES);

            StringValidator
                .of(name + ".Comment", column.getComment())
                .verifyLengthConstraints(1, 1024);
        }

    }

}
