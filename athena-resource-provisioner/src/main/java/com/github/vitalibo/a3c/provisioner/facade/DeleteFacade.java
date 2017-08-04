package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.Facade;
import com.github.vitalibo.a3c.provisioner.model.*;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourcePropertiesTranslator;

public interface DeleteFacade<Request extends ResourceProperties, Response extends ResourceData> extends Facade {

    @Override
    @SuppressWarnings("unchecked")
    default ResourceProviderResponse process(ResourceProviderRequest request) throws AthenaResourceProvisionException {
        final Request resourceProperties =
            ResourcePropertiesTranslator.of(request.getResourceType())
                .from(request.getResourceProperties());

        final Response response = delete(resourceProperties, request.getPhysicalResourceId());

        return ResourceProviderResponse.builder()
            .status(Status.SUCCESS)
            .logicalResourceId(request.getLogicalResourceId())
            .requestId(request.getRequestId())
            .stackId(request.getStackId())
            .physicalResourceId(response.getPhysicalResourceId())
            .data(response)
            .build();
    }

    Response delete(Request request, String physicalResourceId) throws AthenaResourceProvisionException;

}