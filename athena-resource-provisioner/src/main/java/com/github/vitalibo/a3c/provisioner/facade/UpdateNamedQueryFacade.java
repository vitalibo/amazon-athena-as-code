package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionerException;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryResponse;

public class UpdateNamedQueryFacade extends UpdateFacade<NamedQueryRequest, NamedQueryResponse> {

    @Override
    public NamedQueryResponse update(NamedQueryRequest namedQueryRequest, NamedQueryRequest oldResourceRequest) throws AthenaResourceProvisionerException {
        return null;
    }

}
