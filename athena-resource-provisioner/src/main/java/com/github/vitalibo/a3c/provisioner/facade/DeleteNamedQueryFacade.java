package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionerException;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryResponse;

public class DeleteNamedQueryFacade extends DeleteFacade<NamedQueryRequest, NamedQueryResponse> {

    @Override
    public NamedQueryResponse delete(NamedQueryRequest request) throws AthenaResourceProvisionerException {
        return null;
    }

}
