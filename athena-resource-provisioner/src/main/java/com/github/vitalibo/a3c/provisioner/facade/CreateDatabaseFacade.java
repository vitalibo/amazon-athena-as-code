package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.model.DatabaseData;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class CreateDatabaseFacade implements CreateFacade<DatabaseProperties, DatabaseData> {

    private final Collection<Consumer<DatabaseProperties>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<DatabaseProperties> createDatabaseQueryTranslator;

    @Override
    public DatabaseData create(DatabaseProperties properties) throws AthenaProvisionException {
        rules.forEach(rule -> rule.accept(properties));

        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(createDatabaseQueryTranslator.from(properties))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        DatabaseData data = new DatabaseData();
        data.setPhysicalResourceId(properties.getName());
        return data;
    }

}
