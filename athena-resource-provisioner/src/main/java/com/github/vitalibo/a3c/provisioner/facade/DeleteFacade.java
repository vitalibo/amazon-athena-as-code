package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.Facade;
import com.github.vitalibo.a3c.provisioner.model.*;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourcePropertiesTranslator;
import com.github.vitalibo.a3c.provisioner.util.StackUtils;

public interface DeleteFacade<Properties extends ResourceProperties, Data extends ResourceData> extends Facade {

    @Override
    @SuppressWarnings("unchecked")
    default ResourceProviderResponse process(ResourceProviderRequest request) throws AthenaProvisionException {
        final Properties resourceProperties =
            ResourcePropertiesTranslator.of(request.getResourceType())
                .from(request.getResourceProperties());

        if (StackUtils.hasDefaultPhysicalResourceId(request)) {

            return ResourceProviderResponse.builder()
                .status(Status.SUCCESS)
                .logicalResourceId(request.getLogicalResourceId())
                .requestId(request.getRequestId())
                .stackId(request.getStackId())
                .physicalResourceId(request.getPhysicalResourceId())
                .build();
        }

        final Data resourceData = delete(resourceProperties, request.getPhysicalResourceId());

        return ResourceProviderResponse.builder()
            .status(Status.SUCCESS)
            .logicalResourceId(request.getLogicalResourceId())
            .requestId(request.getRequestId())
            .stackId(request.getStackId())
            .physicalResourceId(resourceData.getPhysicalResourceId())
            .data(resourceData)
            .build();
    }

    Data delete(Properties properties, String physicalResourceId) throws AthenaProvisionException;

}
