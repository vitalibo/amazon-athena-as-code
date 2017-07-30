package com.github.vitalibo.a3c.provisioner.model.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;

import java.io.IOException;
import java.io.InputStream;

public class ResourceProviderRequestUnmarshaller {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static ResourceProviderRequest from(InputStream stream) throws IOException {
        ResourceProviderRequest request = MAPPER.readValue(stream, ResourceProviderRequest.class);
        ResourceType resourceType = request.getResourceType();

        request.setResourceProperties(
            MAPPER.convertValue(request.getResourceProperties(), resourceType.getTypeClass()));
        request.setOldResourceProperties(
            MAPPER.convertValue(request.getOldResourceProperties(), resourceType.getTypeClass()));
        return request;
    }

}
