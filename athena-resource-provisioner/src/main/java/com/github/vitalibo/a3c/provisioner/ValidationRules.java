package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.model.Property;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import lombok.Getter;

import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class ValidationRules {

    private ValidationRules() {
        super();
    }

    static void verifyNamedQueryDatabase(NamedQueryProperties o) {
        Validators.getNamedQueryDatabase()
            .verify(o.getDatabase());
    }

    static void verifyNamedQueryDescription(NamedQueryProperties o) {
        Validators.getNamedQueryDescription()
            .verify(o.getDescription());
    }

    static void verifyNamedQueryName(NamedQueryProperties o) {
        Validators.getNamedQueryName()
            .verify(o.getName());
    }

    static void verifyNamedQueryQueryString(NamedQueryProperties o) {
        if (Objects.isNull(o.getQuery())) {
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

        Validators.getNamedQueryNameQueryString()
            .verify(q.getQueryString());
    }

    static void verifyDatabaseName(DatabaseProperties o) {
        Validators.getDatabaseName()
            .verify(o.getName());
    }

    static void verifyDatabaseLocation(DatabaseProperties o) {
        Validators.getDatabaseLocation()
            .verify(o.getLocation());
    }

    static void verifyDatabaseComment(DatabaseProperties o) {
        Validators.getDatabaseComment()
            .verify(o.getComment());
    }

    static void verifyDatabaseProperties(DatabaseProperties o) {
        Validators.getDatabaseProperties()
            .verify(o.getProperties());
    }

    static void verifyTableName(TableProperties o) {
        Validators.getTableName()
            .verify(o.getName());
    }

    static void verifyTableDatabase(TableProperties o) {
        Validators.getTableDatabase()
            .verify(o.getDatabaseName());
    }

    static void verifyTableComment(TableProperties o) {
        Validators.getTableComment()
            .verify(o.getComment());
    }

    static void verifyTableStoredAs(TableProperties o) {
        Validators.getTableStoredAs()
            .verify(o.getStoredAs());
    }

    static void verifyTableLocation(TableProperties o) {
        Validators.getTableLocation()
            .verify(o.getLocation());
    }

    static void verifyTableProperties(TableProperties o) {
        Validators.getTableProperties()
            .verify(o.getProperties());
    }

    static void verifyTableSchema(TableProperties o) {
        Validators.getTableSchema()
            .verify(o.getSchema());
    }

    static void verifyTablePartition(TableProperties o) {
        Validators.getTablePartition()
            .verify(o.getPartition());
    }

    static void verifyTableRowFormat(TableProperties o) {
        TableProperties.RowFormat rw = o.getRowFormat();
        if (Objects.isNull(rw)) {
            return;
        }

        Validators.getTableSerDe()
            .verify(rw.getSerDe());
        Validators.getTableSerDeProperties()
            .verify(rw.getProperties());
    }

    static final class Validators {

        @Getter(lazy = true)
        private static final Validator<String> namedQueryDatabase =
            new StringValidator("Database")
                .verifyRequired()
                .verifyLengthConstraints(1, 32);

        @Getter(lazy = true)
        private static final Validator<String> namedQueryDescription =
            new StringValidator("Description")
                .verifyLengthConstraints(1, 1024);

        @Getter(lazy = true)
        private static final Validator<String> namedQueryName =
            new StringValidator("Name")
                .verifyRequired()
                .verifyLengthConstraints(1, 128);

        @Getter(lazy = true)
        private static final Validator<String> namedQueryNameQueryString =
            new StringValidator("QueryString")
                .verifyLengthConstraints(1, 262144);

        @Getter(lazy = true)
        private static final Validator<String> databaseName =
            new StringValidator("Name")
                .verifyRequired()
                .verifyLengthConstraints(1, 32);

        @Getter(lazy = true)
        private static final Validator<String> databaseLocation =
            new StringValidator("Location")
                .verifyMatchRegex("s3://.*/");

        @Getter(lazy = true)
        private static final Validator<String> databaseComment =
            new StringValidator("Comment")
                .verifyLengthConstraints(1, 1024);

        @Getter(lazy = true)
        private static final Validator<List<Property>> databaseProperties =
            new PropertyValidator("Properties")
                .verifyProperties();

        @Getter(lazy = true)
        private static final Validator<String> tableName =
            new StringValidator("Name")
                .verifyRequired()
                .verifyLengthConstraints(1, 128)
                .verifyMatchRegex("[0-9A-Za-z_]+");

        @Getter(lazy = true)
        private static final Validator<String> tableDatabase =
            new StringValidator("Database")
                .verifyRequired()
                .verifyLengthConstraints(1, 32);

        @Getter(lazy = true)
        private static final Validator<String> tableComment =
            new StringValidator("Comment")
                .verifyLengthConstraints(1, 1024);

        @Getter(lazy = true)
        private static final Validator<String> tableStoredAs =
            new StringValidator("StoredAs")
                .verifyAnyMatch(Arrays.asList(
                    "SEQUENCEFILE", "TEXTFILE", "RCFILE", "ORC",
                    "PARQUET", "AVRO", "INPUTFORMAT `.+` OUTPUTFORMAT `.+`"));

        @Getter(lazy = true)
        private static final Validator<String> tableLocation =
            new StringValidator("Location")
                .verifyMatchRegex("s3://.*/");

        @Getter(lazy = true)
        private static final Validator<List<Property>> tableProperties =
            new PropertyValidator("Properties")
                .verifyProperties();

        @Getter(lazy = true)
        private static final Validator<List<TableProperties.Column>> tableSchema =
            new ColumnValidator("Schema")
                .verifyRequired()
                .verifyColumns();

        @Getter(lazy = true)
        private static final Validator<List<TableProperties.Column>> tablePartition =
            new ColumnValidator("Partition")
                .verifyColumns();

        @Getter(lazy = true)
        private static final Validator<String> tableSerDe =
            new StringValidator("SerDe")
                .verifyRequired();

        @Getter(lazy = true)
        private static final Validator<List<Property>> tableSerDeProperties =
            new PropertyValidator("Properties")
                .verifyProperties();

        private Validators() {
            super();
        }

    }

    static abstract class Validator<Type> {

        protected final String name;
        protected final List<Consumer<Type>> rules;

        Validator(String name) {
            this.name = name;
            this.rules = new ArrayList<>();
        }

        @SuppressWarnings("unchecked")
        <SubClass extends Validator<Type>> SubClass add(Consumer<Type> rule) {
            rules.add(rule);
            return (SubClass) this;
        }

        void verify(Type o) {
            rules.forEach(rule -> rule.accept(o));
        }

    }

    static class StringValidator extends Validator<String> {

        StringValidator(String name) {
            super(name);
        }

        StringValidator verifyRequired() {
            return add(value -> {
                if (StringUtils.isNullOrEmpty(value)) {
                    throw new AthenaProvisionException(String.format(
                        "Required property \"%s\" cannot be null or empty.", name));
                }
            });
        }

        StringValidator verifyLengthConstraints(int min, int max) {
            return add(value -> {
                if (StringUtils.isNullOrEmpty(value)) {
                    return;
                }

                if (value.length() < min || value.length() > max) {
                    throw new AthenaProvisionException(String.format(
                        "The \"%s\" property has length constraints: Minimum length of %d. Maximum length of %d.", name, min, max));
                }
            });
        }

        StringValidator verifyMatchRegex(String regex) {
            return add(value -> {
                if (StringUtils.isNullOrEmpty(value)) {
                    return;
                }

                if (!value.matches(regex)) {
                    throw new AthenaProvisionException(String.format(
                        "The \"%s\" property must match to regex \"%s\"", name, regex));
                }
            });
        }

        StringValidator verifyAnyMatch(List<String> values) {
            final Set<Pattern> patterns = values.stream()
                .map(Pattern::compile).collect(Collectors.toSet());

            return add(value -> {
                if (StringUtils.isNullOrEmpty(value)) {
                    return;
                }

                if (patterns.stream().map(o -> o.matcher(value)).noneMatch(Matcher::matches)) {
                    throw new AthenaProvisionException(String.format(
                        "The value \"%s\" is not allowed for property \"%s\".", value, name));
                }
            });
        }

    }

    static class PropertyValidator extends Validator<List<Property>> {

        private final Map<String, Validator<String>> cache;

        PropertyValidator(String name) {
            super(name);
            this.cache = new HashMap<>();
        }

        PropertyValidator verifyProperties() {
            return add(value -> {
                if (Objects.isNull(value)) {
                    return;
                }

                for (int i = 0; i < value.size(); i++) {
                    verifyProperty(String.format("%s[%d]", name, i), value.get(i));
                }
            });
        }

        private void verifyProperty(String name, Property property) {
            cache.computeIfAbsent(
                name + ".Name", key -> new StringValidator(key)
                    .verifyRequired())
                .verify(property.getName());

            cache.computeIfAbsent(
                name + ".Value", key -> new StringValidator(key)
                    .verifyRequired())
                .verify(property.getValue());
        }

    }

    static class ColumnValidator extends Validator<List<TableProperties.Column>> {

        private static final List<String> KNOWN_COLUMN_TYPES = Arrays.asList(
            "TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "DOUBLE",
            "STRING", "BINARY", "TIMESTAMP", "DECIMAL.*", "DATE", "VARCHAR",
            "ARRAY.+", "MAP.+", "STRUCT.+");

        private final Map<String, Validator<String>> cache;

        ColumnValidator(String name) {
            super(name);
            this.cache = new HashMap<>();
        }

        ColumnValidator verifyRequired() {
            return add(columns -> {
                if (Objects.isNull(columns) || columns.isEmpty()) {
                    throw new AthenaProvisionException(String.format(
                        "Required property \"%s\" cannot be null or empty.", name));
                }
            });
        }

        ColumnValidator verifyColumns() {
            return add(value -> {
                if (Objects.isNull(value)) {
                    return;
                }

                for (int i = 0; i < value.size(); i++) {
                    verifyColumn(String.format("%s[%d]", name, i), value.get(i));
                }
            });
        }

        private void verifyColumn(String name, TableProperties.Column column) {
            cache.computeIfAbsent(
                name + ".Name", key -> new StringValidator(key)
                    .verifyRequired())
                .verify(column.getName());

            cache.computeIfAbsent(
                name + ".Type", key -> new StringValidator(key)
                    .verifyRequired()
                    .verifyAnyMatch(KNOWN_COLUMN_TYPES))
                .verify(column.getType());

            cache.computeIfAbsent(
                name + ".Comment", key -> new StringValidator(key)
                    .verifyLengthConstraints(1, 1024))
                .verify(column.getComment());
        }

    }

}
