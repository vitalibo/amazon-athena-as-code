package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.DatabaseRequest;
import com.github.vitalibo.a3c.provisioner.model.DatabaseResponse;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.function.BiConsumer;

@RequiredArgsConstructor
public class UpdateDatabaseFacade implements UpdateFacade<DatabaseRequest, DatabaseResponse> {

    private final Collection<BiConsumer<DatabaseRequest, DatabaseRequest>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final ResultConfiguration athenaResultConfiguration;
    private final QueryStringTranslator<DatabaseRequest> createDatabaseQueryTranslator;
    private final QueryStringTranslator<DatabaseRequest> updateDatabasePropertiesQueryTranslator;

    @Override
    public DatabaseResponse update(DatabaseRequest request, DatabaseRequest oldRequest,
                                   String physicalResourceId) throws AthenaResourceProvisionException {
        rules.forEach(rule -> rule.accept(request, oldRequest));

        final String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(
                    chooseQueryStringTranslatorStrategy(request.getName(), physicalResourceId).from(request))
                .withResultConfiguration(athenaResultConfiguration))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        DatabaseResponse response = new DatabaseResponse();
        response.setPhysicalResourceId(request.getName());
        return response;
    }

    private QueryStringTranslator<DatabaseRequest> chooseQueryStringTranslatorStrategy(String databaseName,
                                                                                       String physicalResourceId) {
        return databaseName.equals(physicalResourceId) ?
            updateDatabasePropertiesQueryTranslator : createDatabaseQueryTranslator;
    }

}
