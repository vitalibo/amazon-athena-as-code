package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.Facade;
import com.github.vitalibo.a3c.provisioner.model.*;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourcePropertiesTranslator;

public interface UpdateFacade<Properties extends ResourceProperties, Data extends ResourceData> extends Facade {

    @Override
    @SuppressWarnings("unchecked")
    default ResourceProviderResponse process(ResourceProviderRequest request) throws AthenaResourceProvisionException {
        final Properties resourceProperties =
            ResourcePropertiesTranslator.of(request.getResourceType())
                .from(request.getResourceProperties());

        final Properties oldResourceProperties;
        try {
            oldResourceProperties =
                ResourcePropertiesTranslator.of(request.getResourceType())
                    .from(request.getOldResourceProperties());

        } catch (AthenaResourceProvisionException ignored) {
            // When status UPDATE_ROLLBACK_IN_PROGRESS
            return ResourceProviderResponse.builder()
                .status(Status.SUCCESS)
                .logicalResourceId(request.getLogicalResourceId())
                .requestId(request.getRequestId())
                .stackId(request.getStackId())
                .physicalResourceId(request.getPhysicalResourceId())
                .build();
        }

        final Data resourceData = update(
            resourceProperties, oldResourceProperties, request.getPhysicalResourceId());

        return ResourceProviderResponse.builder()
            .status(Status.SUCCESS)
            .logicalResourceId(request.getLogicalResourceId())
            .requestId(request.getRequestId())
            .stackId(request.getStackId())
            .physicalResourceId(resourceData.getPhysicalResourceId())
            .data(resourceData)
            .build();
    }

    Data update(Properties properties, Properties oldProperties, String physicalResourceId) throws AthenaResourceProvisionException;

}
