package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.Facade;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderResponse;
import com.github.vitalibo.a3c.provisioner.model.ResponseData;
import com.github.vitalibo.a3c.provisioner.model.ResponseStatus;

public interface UpdateFacade<Request, Response extends ResponseData> extends Facade {

    @Override
    @SuppressWarnings("unchecked")
    default ResourceProviderResponse process(ResourceProviderRequest request) throws AthenaResourceProvisionException {
        final Response response = update(
            (Request) request.getResourceProperties(),
            (Request) request.getOldResourceProperties(),
            request.getPhysicalResourceId());

        return ResourceProviderResponse.builder()
            .status(ResponseStatus.SUCCESS)
            .logicalResourceId(request.getLogicalResourceId())
            .requestId(request.getRequestId())
            .stackId(request.getStackId())
            .physicalResourceId(response.getPhysicalResourceId())
            .data(response)
            .build();
    }

    Response update(Request request, Request oldResourceRequest, String physicalResourceId) throws AthenaResourceProvisionException;

}
