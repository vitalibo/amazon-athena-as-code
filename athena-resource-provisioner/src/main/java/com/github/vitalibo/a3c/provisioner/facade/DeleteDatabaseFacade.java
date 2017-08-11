package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.DatabaseRequest;
import com.github.vitalibo.a3c.provisioner.model.DatabaseResponse;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DeleteDatabaseFacade implements DeleteFacade<DatabaseRequest, DatabaseResponse> {

    private final AmazonAthenaSync amazonAthena;
    private final ResultConfiguration athenaResultConfiguration;
    private final QueryStringTranslator<DatabaseRequest> dropDatabaseQueryTranslator;

    @Override
    public DatabaseResponse delete(DatabaseRequest request, String physicalResourceId) throws AthenaResourceProvisionException {
        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(dropDatabaseQueryTranslator.from(request))
                .withResultConfiguration(athenaResultConfiguration))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        DatabaseResponse response = new DatabaseResponse();
        response.setPhysicalResourceId(physicalResourceId);
        return response;
    }

}
