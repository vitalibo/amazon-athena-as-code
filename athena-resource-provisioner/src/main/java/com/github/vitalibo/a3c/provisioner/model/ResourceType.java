package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourceTypeDeserializer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.stream.Stream;

@RequiredArgsConstructor
@JsonDeserialize(using = ResourceTypeDeserializer.class)
public enum ResourceType {

    NamedQuery("Custom::AthenaNamedQuery", NamedQueryRequest.class);

    @Getter
    private final String name;
    @Getter
    private final Class<? extends RequestProperties> typeClass;

    public static ResourceType of(String name) {
        return Stream.of(values())
            .filter(o -> name.equals(o.name))
            .findFirst().orElseThrow(IllegalArgumentException::new);
    }

}