package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.Facade;
import com.github.vitalibo.a3c.provisioner.model.*;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourcePropertiesTranslator;
import com.github.vitalibo.a3c.provisioner.util.Verify;

public interface UpdateFacade<Properties extends ResourceProperties, Data extends ResourceData> extends Facade, Verify<Properties> {

    @Override
    default ResourceProviderResponse process(ResourceProviderRequest request) throws AthenaProvisionException {
        final Properties resourceProperties =
            ResourcePropertiesTranslator.of(request.getResourceType())
                .from(request.getResourceProperties());

        final Properties oldResourceProperties;
        try {
            oldResourceProperties =
                ResourcePropertiesTranslator.of(request.getResourceType())
                    .from(request.getOldResourceProperties());

            verify(oldResourceProperties);
        } catch (AthenaProvisionException ignored) {
            // When status UPDATE_ROLLBACK_IN_PROGRESS
            return ResourceProviderResponse.builder()
                .status(Status.SUCCESS)
                .logicalResourceId(request.getLogicalResourceId())
                .requestId(request.getRequestId())
                .stackId(request.getStackId())
                .physicalResourceId(request.getPhysicalResourceId())
                .build();
        }

        verify(resourceProperties);
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

    Data update(Properties properties, Properties oldProperties, String physicalResourceId) throws AthenaProvisionException;

}
