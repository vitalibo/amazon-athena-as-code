package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.util.json.Jackson;
import lombok.experimental.UtilityClass;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

@UtilityClass
public class TestHelper {

    public static String resourceAsJsonString(String resource) {
        return Jackson.toJsonString(
            Jackson.fromJsonString(
                resourceAsString(resource), Object.class));
    }

    public static String resourceAsString(String resource) {
        return new BufferedReader(new InputStreamReader(TestHelper.class.getResourceAsStream(resource)))
            .lines().collect(Collectors.joining(System.lineSeparator()));
    }

}