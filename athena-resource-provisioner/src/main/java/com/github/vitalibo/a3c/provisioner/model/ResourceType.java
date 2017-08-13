package com.github.vitalibo.a3c.provisioner.model;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourceTypeDeserializer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.stream.Stream;

@RequiredArgsConstructor
@JsonDeserialize(using = ResourceTypeDeserializer.class)
public enum ResourceType {

    Unknown(null, ResourceProperties.class),
    NamedQuery("Custom::AthenaNamedQuery", NamedQueryProperties.class),
    Database("Custom::AthenaDatabase", DatabaseProperties.class),
    Table("Custom::AthenaTable", TableProperties.class);

    @Getter(onMethod = @__(@JsonValue))
    private final String name;
    @Getter
    private final Class<? extends ResourceProperties> typeClass;

    public static ResourceType of(String name) {
        return Stream.of(values())
            .filter(o -> name.equals(o.name))
            .findFirst().orElse(Unknown);
    }

}