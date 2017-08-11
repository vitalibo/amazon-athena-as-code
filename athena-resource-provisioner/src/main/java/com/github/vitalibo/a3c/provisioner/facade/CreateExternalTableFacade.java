package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableRequest;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableResponse;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CreateExternalTableFacade implements CreateFacade<ExternalTableRequest, ExternalTableResponse> {

    @Override
    public ExternalTableResponse create(ExternalTableRequest externalTableRequest) throws AthenaResourceProvisionException {
        return null;
    }

}
