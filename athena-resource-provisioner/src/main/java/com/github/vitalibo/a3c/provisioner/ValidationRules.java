package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.model.Property;

import java.util.List;

class ValidationRules {

    private ValidationRules() {
        super();
    }

    static void verifyDatabase(NamedQueryProperties namedQueryProperties) {
        String database = namedQueryProperties.getDatabase();
        if (StringUtils.isNullOrEmpty(database)) {
            throw new AthenaProvisionException(
                "Required property \"Database\" cannot be null or empty.");
        }

        if (!inRange(database.length(), 1, 32)) {
            throw new AthenaProvisionException(
                "The \"Database\" property has length constraints: Minimum length of 1. Maximum length of 32.");
        }
    }

    static void verifyDescription(NamedQueryProperties namedQueryProperties) {
        String description = namedQueryProperties.getDescription();
        if (StringUtils.isNullOrEmpty(description)) {
            return;
        }

        if (!inRange(description.length(), 1, 1024)) {
            throw new AthenaProvisionException(
                "The \"Description\" property has length constraints: Minimum length of 1. Maximum length of 1024.");
        }
    }

    static void verifyName(NamedQueryProperties namedQueryProperties) {
        String name = namedQueryProperties.getName();
        if (StringUtils.isNullOrEmpty(name)) {
            throw new AthenaProvisionException(
                "Required property \"Name\" cannot be null or empty.");
        }

        if (!inRange(name.length(), 1, 128)) {
            throw new AthenaProvisionException(
                "The \"Name\" property has length constraints: Minimum length of 1. Maximum length of 128.");
        }
    }

    static void verifyQueryString(NamedQueryProperties namedQueryProperties) {
        NamedQueryProperties.Query query = namedQueryProperties.getQuery();
        if (query == null) {
            throw new AthenaProvisionException(
                "Required property \"QueryString\" cannot be null or empty.");
        }

        if (StringUtils.isNullOrEmpty(query.getQueryString())) {
            if (StringUtils.isNullOrEmpty(query.getS3Bucket()) ||
                StringUtils.isNullOrEmpty(query.getS3Key())) {
                throw new AthenaProvisionException(
                    "The properties \"S3Bucket\" and \"S3Key\" must be present.");
            }

            return;
        }

        if (!inRange(query.getQueryString().length(), 1, 262144)) {
            throw new AthenaProvisionException(
                "The \"QueryString\" property has length constraints: Minimum length of 1. Maximum length of 262144.");
        }
    }

    static void verifyName(DatabaseProperties databaseProperties) {
        String name = databaseProperties.getName();
        if (StringUtils.isNullOrEmpty(name)) {
            throw new AthenaProvisionException(
                "Required property \"Name\" cannot be null or empty.");
        }

        if (!inRange(name.length(), 1, 32)) {
            throw new AthenaProvisionException(
                "The \"Name\" property has length constraints: Minimum length of 1. Maximum length of 32.");
        }
    }

    static void verifyLocation(DatabaseProperties databaseProperties) {
        String location = databaseProperties.getLocation();
        if (StringUtils.isNullOrEmpty(location)) {
            return;
        }

        if (!location.matches("s3://.*/")) {
            throw new AthenaProvisionException(
                "The \"Location\" property must match to pattern 's3://.*/'.");
        }
    }

    static void verifyComment(DatabaseProperties databaseProperties) {
        String comment = databaseProperties.getComment();
        if (StringUtils.isNullOrEmpty(comment)) {
            return;
        }

        if (!inRange(comment.length(), 1, 1024)) {
            throw new AthenaProvisionException(
                "The \"Comment\" property has length constraints: Minimum length of 1. Maximum length of 1024.");
        }
    }

    static void verifyProperties(DatabaseProperties databaseProperties) {
        List<Property> properties = databaseProperties.getProperties();
        if (properties == null) {
            return;
        }

        for (int i = 0; i < properties.size(); i++) {
            verifyProperty(i, properties.get(i));
        }
    }

    private static void verifyProperty(int index, Property property) {
        if (StringUtils.isNullOrEmpty(property.getName())) {
            throw new AthenaProvisionException(
                "Required property \"Properties[" + index + "].Name\" cannot be null or empty.");
        }

        if (StringUtils.isNullOrEmpty(property.getValue())) {
            throw new AthenaProvisionException(
                "Required property \"Properties[" + index + "].Value\" cannot be null or empty.");
        }
    }

    private static boolean inRange(int length, int min, int max) {
        return length >= min && length <= max;
    }

}
