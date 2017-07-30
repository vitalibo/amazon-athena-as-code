package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderResponse;
import com.github.vitalibo.a3c.provisioner.model.transform.ResourceProviderRequestUnmarshaller;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

        ResourceProviderResponse response = facade.process(request);

        ResponseSender sender = factory.createResponseSender();
        sender.send(response);
    }

}
