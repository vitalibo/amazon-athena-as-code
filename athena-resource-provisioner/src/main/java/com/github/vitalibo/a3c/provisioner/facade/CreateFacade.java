package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionerException;
import com.github.vitalibo.a3c.provisioner.Facade;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderResponse;
import com.github.vitalibo.a3c.provisioner.model.ResponseData;
import com.github.vitalibo.a3c.provisioner.model.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CreateFacade<Request, Response extends ResponseData> implements Facade {

    private static final Logger logger = LoggerFactory.getLogger(CreateFacade.class);

    @Override
    public ResourceProviderResponse process(ResourceProviderRequest request) {
        try {
            final Response response = create((Request) request.getResourceProperties());

            return new ResourceProviderResponse()
                .withStatus(ResponseStatus.SUCCESS)
                .withLogicalResourceId(request.getLogicalResourceId())
                .withRequestId(request.getRequestId())
                .withStackId(request.getStackId())
                .withPhysicalResourceId(response.getPhysicalResourceId())
                .withData(response);

        } catch (AthenaResourceProvisionerException e) {
            logger.error(e.getMessage(), e);
            return new ResourceProviderResponse()
                .withStatus(ResponseStatus.FAILED)
                .withReason(e.getMessage())
                .withLogicalResourceId(request.getLogicalResourceId())
                .withRequestId(request.getRequestId())
                .withStackId(request.getStackId());
        }
    }

    public abstract Response create(Request request) throws AthenaResourceProvisionerException;

}
