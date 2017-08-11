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
import java.util.function.BiConsumer;

@RequiredArgsConstructor
public class UpdateExternalTableFacade implements UpdateFacade<ExternalTableRequest, ExternalTableResponse> {

    private final Collection<BiConsumer<ExternalTableRequest, ExternalTableRequest>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final ResultConfiguration athenaResultConfiguration;
    private final QueryStringTranslator<ExternalTableRequest> createTableQueryTranslator;
    private final QueryStringTranslator<ExternalTableRequest> dropTableQueryTranslator;

    @Override
    public ExternalTableResponse update(ExternalTableRequest request, ExternalTableRequest oldRequest,
                                        String physicalResourceId) throws AthenaResourceProvisionException {
        rules.forEach(rule -> rule.accept(request, oldRequest));

        if (request.getName().equals(physicalResourceId)) {
            String queryExecutionId = amazonAthena.startQueryExecution(
                new StartQueryExecutionRequest()
                    .withQueryString(dropTableQueryTranslator.from(oldRequest))
                    .withQueryExecutionContext(new QueryExecutionContext()
                        .withDatabase(oldRequest.getDatabaseName()))
                    .withResultConfiguration(athenaResultConfiguration))
                .getQueryExecutionId();

            amazonAthena.waitQueryExecution(queryExecutionId);
        }

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
