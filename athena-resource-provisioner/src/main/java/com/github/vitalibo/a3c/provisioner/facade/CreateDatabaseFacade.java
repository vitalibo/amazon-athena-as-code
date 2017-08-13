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
import java.util.function.Consumer;

@RequiredArgsConstructor
public class CreateDatabaseFacade implements CreateFacade<DatabaseRequest, DatabaseResponse> {

    private final Collection<Consumer<DatabaseRequest>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<DatabaseRequest> createDatabaseQueryTranslator;

    @Override
    public DatabaseResponse create(DatabaseRequest request) throws AthenaResourceProvisionException {
        rules.forEach(rule -> rule.accept(request));

        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(createDatabaseQueryTranslator.from(request))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        DatabaseResponse response = new DatabaseResponse();
        response.setPhysicalResourceId(request.getName());
        return response;
    }

}
