package com.github.vitalibo.a3c.provisioner.model.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.vitalibo.a3c.provisioner.model.RequestProperties;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class ResourceProviderRequestUnmarshaller {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ResourceProviderRequest from(InputStream stream) throws IOException {
        ResourceProviderRequest request = objectMapper.readValue(stream, ResourceProviderRequest.class);
        ResourceType resourceType = request.getResourceType();

        class Helper {

            private RequestProperties convertToResourceType(Object resourceProperties) {
                return Optional.ofNullable(objectMapper.convertValue(resourceProperties, resourceType.getTypeClass()))
                    .map(o -> {
                        o.setPhysicalResourceId(request.getPhysicalResourceId());
                        return o;
                    }).orElse(null);
            }
        }
        Helper o = new Helper();

        request.setResourceProperties(
            o.convertToResourceType(request.getResourceProperties()));
        request.setOldResourceProperties(
            o.convertToResourceType(request.getOldResourceProperties()));
        return request;
    }


}
