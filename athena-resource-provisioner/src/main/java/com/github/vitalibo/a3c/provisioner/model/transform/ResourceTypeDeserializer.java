package com.github.vitalibo.a3c.provisioner.model.transform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;

import java.io.IOException;

public class ResourceTypeDeserializer extends JsonDeserializer<ResourceType> {

    @Override
    public ResourceType deserialize(JsonParser jp, DeserializationContext context) throws IOException {
        return ResourceType.of(((TextNode) jp.getCodec().readTree(jp)).asText());
    }

}
