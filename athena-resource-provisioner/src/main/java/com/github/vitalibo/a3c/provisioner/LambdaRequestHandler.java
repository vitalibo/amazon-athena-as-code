package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.util.json.Jackson;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderResponse;
import com.github.vitalibo.a3c.provisioner.model.ResponseStatus;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourceProviderRequestUnmarshaller;
import com.github.vitalibo.a3c.provisioner.util.S3PreSignedURL;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

@RequiredArgsConstructor
public class LambdaRequestHandler implements RequestStreamHandler {

    private final Factory factory;

    public LambdaRequestHandler() {
        this(Factory.getInstance());
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ResourceProviderRequest request = ResourceProviderRequestUnmarshaller.from(input);

        final Facade facade;
        switch (request.getRequestType()) {
            case Create:
                facade = factory.createCreateFacade(request);
                break;
            case Delete:
                facade = factory.createDeleteFacade(request);
                break;
            case Update:
                facade = factory.createUpdateFacade(request);
                break;
            default:
                throw new IllegalStateException();
        }

        ResourceProviderResponse response;
        try {
            response = facade.process(request);
        } catch (AthenaResourceProvisionException e) {
            response = ResourceProviderResponse.builder()
                .status(ResponseStatus.FAILED)
                .reason(e.getMessage())
                .logicalResourceId(request.getLogicalResourceId())
                .requestId(request.getRequestId())
                .stackId(request.getStackId())
                .physicalResourceId(request.getPhysicalResourceId())
                .build();
        }

        String responseJson = Jackson.toJsonString(response);
        S3PreSignedURL preSignedUrl = new S3PreSignedURL(request.getResponseUrl());
        preSignedUrl.upload(responseJson);

        try (OutputStreamWriter writer = new OutputStreamWriter(output)) {
            writer.write(responseJson);
            writer.flush();
        }
    }

}
