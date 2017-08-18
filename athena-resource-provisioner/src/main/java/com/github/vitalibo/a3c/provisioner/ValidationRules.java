package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;

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

    private static boolean inRange(int length, int min, int max) {
        return length >= min && length <= max;
    }

}
