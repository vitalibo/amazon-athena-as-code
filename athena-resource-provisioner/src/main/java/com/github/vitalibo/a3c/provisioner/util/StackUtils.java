package com.github.vitalibo.a3c.provisioner.util;

import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StackUtils {

    private static final Pattern CLOUD_FORMATION_STACK_ARN_PATTERN = Pattern.compile(
        "arn:aws:cloudformation:[-A-Za-z0-9]+:[A-Za-z0-9]+:stack/(?<name>[-A-Za-z0-9]+)/.+");

    private StackUtils() {
        super();
    }

    public static boolean hasDefaultPhysicalResourceId(ResourceProviderRequest request) {
        Matcher matcher = CLOUD_FORMATION_STACK_ARN_PATTERN.matcher(request.getStackId());
        if (matcher.matches()) {
            return request.getPhysicalResourceId()
                .matches(String.format("%s-%s-.+", matcher.group("name"), request.getLogicalResourceId()));
        }

        throw new IllegalStateException();
    }

    public static String makeDefaultPhysicalResourceId(ResourceProviderRequest request) {
        Matcher matcher = CLOUD_FORMATION_STACK_ARN_PATTERN.matcher(request.getStackId());
        if (matcher.matches()) {
            return String.format("%s-%s-%s",
                matcher.group("name"), request.getLogicalResourceId(), UUID.randomUUID());
        }

        throw new IllegalStateException();
    }

}
