package com.github.vitalibo.a3c.provisioner.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.InputStream;

public enum Jackson {

    ;

    @Getter
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @SneakyThrows
    public static String toJsonString(Object value) {
        return objectMapper.writeValueAsString(value);
    }

    @SneakyThrows
    public static <T> T fromJsonString(String json, Class<T> clazz) {
        return objectMapper.readValue(json, clazz);
    }

    @SneakyThrows
    public static <T> T fromJsonString(InputStream stream, Class<T> clazz) {
        return objectMapper.readValue(stream, clazz);
    }

    @SneakyThrows
    public static <T> T fromJsonString(String json, TypeReference<T> type) {
        return objectMapper.readValue(json, type);
    }

    @SneakyThrows
    public static <T> T fromJsonString(InputStream stream, TypeReference<T> type) {
        return objectMapper.readValue(stream, type);
    }

    @SneakyThrows
    public static <T> T convertValue(Object object, Class<T> clazz) {
        return objectMapper.convertValue(object, clazz);
    }

    @SneakyThrows
    public static <T> T convertValue(Object object, TypeReference<T> type) {
        return objectMapper.convertValue(object, type);
    }

}
