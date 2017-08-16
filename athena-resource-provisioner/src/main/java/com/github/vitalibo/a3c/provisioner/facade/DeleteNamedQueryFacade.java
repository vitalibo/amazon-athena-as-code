package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.DeleteNamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryData;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DeleteNamedQueryFacade implements DeleteFacade<NamedQueryProperties, NamedQueryData> {

    private final AmazonAthena amazonAthena;

    @Override
    public NamedQueryData delete(NamedQueryProperties properties, String physicalResourceId) throws AthenaProvisionException {
        amazonAthena.deleteNamedQuery(
            new DeleteNamedQueryRequest()
                .withNamedQueryId(physicalResourceId));

        NamedQueryData data = new NamedQueryData();
        data.setPhysicalResourceId(physicalResourceId);
        return data;
    }

}
