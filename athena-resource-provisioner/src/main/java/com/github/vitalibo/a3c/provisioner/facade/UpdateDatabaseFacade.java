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
public class UpdateDatabaseFacade implements UpdateFacade<DatabaseProperties, DatabaseData> {

    @Delegate
    private final Rules<DatabaseProperties> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<DatabaseProperties> createDatabaseQueryTranslator;
    private final QueryStringTranslator<DatabaseProperties> updateDatabasePropertiesQueryTranslator;

    @Override
    public DatabaseData update(DatabaseProperties properties, DatabaseProperties oldProperties,
                               String physicalResourceId) throws AthenaProvisionException {
        // TODO: Add support update 'Comment' and 'Location'
        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(
                    chooseQueryStringTranslatorStrategy(properties.getName(), physicalResourceId).from(properties))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        return new DatabaseData()
            .withName(properties.getName())
            .withPhysicalResourceId(properties.getName());
    }

    private QueryStringTranslator<DatabaseProperties> chooseQueryStringTranslatorStrategy(String databaseName,
                                                                                          String physicalResourceId) {
        return databaseName.equalsIgnoreCase(physicalResourceId) ?
            updateDatabasePropertiesQueryTranslator : createDatabaseQueryTranslator;
    }

}
