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
public class UpdateDatabaseFacade implements UpdateFacade<DatabaseProperties, DatabaseData> {

    private final Collection<Consumer<DatabaseProperties>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<DatabaseProperties> createDatabaseQueryTranslator;
    private final QueryStringTranslator<DatabaseProperties> updateDatabasePropertiesQueryTranslator;

    @Override
    public DatabaseData update(DatabaseProperties properties, DatabaseProperties oldProperties,
                               String physicalResourceId) throws AthenaProvisionException {
        try {
            rules.forEach(rule -> rule.accept(oldProperties));
        } catch (AthenaProvisionException ignore) {
            return new DatabaseData()
                .withPhysicalResourceId(physicalResourceId);
        }
        rules.forEach(rule -> rule.accept(properties));

        final String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(
                    chooseQueryStringTranslatorStrategy(properties.getName(), physicalResourceId).from(properties))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        return new DatabaseData()
            .withPhysicalResourceId(properties.getName()
                .toLowerCase());
    }

    private QueryStringTranslator<DatabaseProperties> chooseQueryStringTranslatorStrategy(String databaseName,
                                                                                          String physicalResourceId) {
        return databaseName.equals(physicalResourceId) ?
            updateDatabasePropertiesQueryTranslator : createDatabaseQueryTranslator;
    }

}
