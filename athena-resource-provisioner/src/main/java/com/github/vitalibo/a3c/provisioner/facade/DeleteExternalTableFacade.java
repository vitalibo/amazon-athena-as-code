package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableRequest;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableResponse;
import com.github.vitalibo.a3c.provisioner.model.transform.EncryptionConfigurationTranslator;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DeleteExternalTableFacade implements DeleteFacade<ExternalTableRequest, ExternalTableResponse> {

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<ExternalTableRequest> dropTableQueryTranslator;

    @Override
    public ExternalTableResponse delete(ExternalTableRequest request, String physicalResourceId) throws AthenaResourceProvisionException {
        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(dropTableQueryTranslator.from(request))
                .withQueryExecutionContext(new QueryExecutionContext()
                    .withDatabase(request.getDatabaseName()))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)
                    .withEncryptionConfiguration(
                        EncryptionConfigurationTranslator.from(request))))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        ExternalTableResponse response = new ExternalTableResponse();
        response.setPhysicalResourceId(physicalResourceId);
        return response;
    }

}
