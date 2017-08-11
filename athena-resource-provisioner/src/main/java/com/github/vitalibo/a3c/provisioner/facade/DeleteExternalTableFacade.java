package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableRequest;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableResponse;

public class DeleteExternalTableFacade implements DeleteFacade<ExternalTableRequest, ExternalTableResponse> {

    @Override
    public ExternalTableResponse delete(ExternalTableRequest externalTableRequest,
                                        String physicalResourceId) throws AthenaResourceProvisionException {
        return null;
    }

}
