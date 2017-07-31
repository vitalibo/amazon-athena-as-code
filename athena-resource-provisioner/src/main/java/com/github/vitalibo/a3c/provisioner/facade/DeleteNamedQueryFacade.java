package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.DeleteNamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryResponse;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DeleteNamedQueryFacade implements DeleteFacade<NamedQueryRequest, NamedQueryResponse> {

    private final AmazonAthena amazonAthena;

    @Override
    public NamedQueryResponse delete(NamedQueryRequest request, String physicalResourceId) throws AthenaResourceProvisionException {
        amazonAthena.deleteNamedQuery(
            new DeleteNamedQueryRequest()
                .withNamedQueryId(physicalResourceId));

        NamedQueryResponse response = new NamedQueryResponse();
        response.setPhysicalResourceId(physicalResourceId);
        return response;
    }

}
