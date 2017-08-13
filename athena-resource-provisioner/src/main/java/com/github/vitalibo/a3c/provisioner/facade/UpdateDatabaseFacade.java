package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.DatabaseData;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.function.BiConsumer;

@RequiredArgsConstructor
public class UpdateDatabaseFacade implements UpdateFacade<DatabaseProperties, DatabaseData> {

    private final Collection<BiConsumer<DatabaseProperties, DatabaseProperties>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<DatabaseProperties> createDatabaseQueryTranslator;
    private final QueryStringTranslator<DatabaseProperties> updateDatabasePropertiesQueryTranslator;

    @Override
    public DatabaseData update(DatabaseProperties properties, DatabaseProperties oldProperties,
                               String physicalResourceId) throws AthenaResourceProvisionException {
        rules.forEach(rule -> rule.accept(properties, oldProperties));

        final String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(
                    chooseQueryStringTranslatorStrategy(properties.getName(), physicalResourceId).from(properties))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        DatabaseData data = new DatabaseData();
        data.setPhysicalResourceId(properties.getName());
        return data;
    }

    private QueryStringTranslator<DatabaseProperties> chooseQueryStringTranslatorStrategy(String databaseName,
                                                                                          String physicalResourceId) {
        return databaseName.equals(physicalResourceId) ?
            updateDatabasePropertiesQueryTranslator : createDatabaseQueryTranslator;
    }

}
