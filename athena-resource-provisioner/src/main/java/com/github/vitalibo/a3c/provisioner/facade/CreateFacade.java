package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.Facade;
import com.github.vitalibo.a3c.provisioner.model.*;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourcePropertiesTranslator;

import java.util.UUID;
import java.util.function.Consumer;

public interface CreateFacade<Properties extends ResourceProperties, Data extends ResourceData> extends Facade, Consumer<Properties> {

    @Override
    @SuppressWarnings("unchecked")
    default ResourceProviderResponse process(ResourceProviderRequest request) throws AthenaProvisionException {
        request.setPhysicalResourceId(
            request.getLogicalResourceId() + "-" + UUID.randomUUID());

        final Properties resourceProperties =
            ResourcePropertiesTranslator.of(request.getResourceType())
                .from(request.getResourceProperties());

        accept(resourceProperties);
        final Data resourceData = create(resourceProperties);

        return ResourceProviderResponse.builder()
            .status(Status.SUCCESS)
            .logicalResourceId(request.getLogicalResourceId())
            .requestId(request.getRequestId())
            .stackId(request.getStackId())
            .physicalResourceId(resourceData.getPhysicalResourceId())
            .data(resourceData)
            .build();
    }

    Data create(Properties properties) throws AthenaProvisionException;

}
