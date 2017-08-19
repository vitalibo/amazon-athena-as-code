package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.model.DatabaseData;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import com.github.vitalibo.a3c.provisioner.util.Rules;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

@RequiredArgsConstructor
public class CreateDatabaseFacade implements CreateFacade<DatabaseProperties, DatabaseData> {

    @Delegate
    private final Rules<DatabaseProperties> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<DatabaseProperties> createDatabaseQueryTranslator;

    @Override
    public DatabaseData create(DatabaseProperties properties) throws AthenaProvisionException {
        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(createDatabaseQueryTranslator.from(properties))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        return new DatabaseData()
            .withPhysicalResourceId(properties.getName()
                .toLowerCase());
    }

}
