package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableRequest;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableResponse;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class CreateExternalTableFacade implements CreateFacade<ExternalTableRequest, ExternalTableResponse> {

    private final Collection<Consumer<ExternalTableRequest>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final ResultConfiguration athenaResultConfiguration;
    private final QueryStringTranslator<ExternalTableRequest> createTableQueryTranslator;

    @Override
    public ExternalTableResponse create(ExternalTableRequest request) throws AthenaResourceProvisionException {
        rules.forEach(rule -> rule.accept(request));

        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(createTableQueryTranslator.from(request))
                .withQueryExecutionContext(new QueryExecutionContext()
                    .withDatabase(request.getDatabaseName()))
                .withResultConfiguration(athenaResultConfiguration))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        ExternalTableResponse response = new ExternalTableResponse();
        response.setPhysicalResourceId(request.getName());
        return response;
    }

}
