package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableRequest;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableResponse;

public class UpdateExternalTableFacade implements UpdateFacade<ExternalTableRequest, ExternalTableResponse> {

    @Override
    public ExternalTableResponse update(ExternalTableRequest externalTableRequest, ExternalTableRequest oldResourceRequest,
                                        String physicalResourceId) throws AthenaResourceProvisionException {
        return null;
    }

}
