package com.github.vitalibo.a3c.provisioner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Map;

public class JacksonTest {

    @Test
    public void testGetObjectMapper() {
        ObjectMapper actual = Jackson.getObjectMapper();

        Assert.assertNotNull(actual);
    }

    @Test
    public void testToJsonString() {
        String actual = Jackson.toJsonString(Collections.singletonMap("foo", "bar"));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, "{\"foo\":\"bar\"}");
    }

    @Test
    public void testFromJsonString() {
        Map actual = Jackson.fromJsonString("{\"foo\":\"bar\"}", Map.class);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get("foo"), "bar");
    }

    @Test
    public void testFromJsonInputStream() {
        Map actual = Jackson.fromJsonString(new ByteArrayInputStream("{\"foo\":\"bar\"}".getBytes()), Map.class);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get("foo"), "bar");
    }

    @Test
    public void testFromJsonStringTypeReference() {
        Map<String, String> actual = Jackson.fromJsonString(
            "{\"foo\":\"bar\"}",
            new TypeReference<Map<String, String>>() {
            });

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get("foo"), "bar");
    }

    @Test
    public void testFromJsonInputStreamTypeReference() {
        Map<String, String> actual = Jackson.fromJsonString(
            new ByteArrayInputStream("{\"foo\":\"bar\"}".getBytes()),
            new TypeReference<Map<String, String>>() {
            });

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get("foo"), "bar");
    }

    @Test
    public void testConvertValue() {
        Sample actual = Jackson.convertValue(Collections.singletonMap("foo", "bar"), Sample.class);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, new Sample("bar"));
    }

    @Test
    public void testConvertValueTypeReference() {
        Sample actual = Jackson.convertValue(
            Collections.singletonMap("foo", "bar"),
            new TypeReference<Sample>() {
            });

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, new Sample("bar"));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Sample {

        private String foo;

    }

}