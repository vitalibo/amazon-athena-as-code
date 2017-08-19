package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.model.TableData;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.EncryptionConfigurationTranslator;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import com.github.vitalibo.a3c.provisioner.util.Rules;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

@RequiredArgsConstructor
public class UpdateTableFacade implements UpdateFacade<TableProperties, TableData> {

    @Delegate
    private final Rules<TableProperties> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<TableProperties> createTableQueryTranslator;
    private final QueryStringTranslator<TableProperties> dropTableQueryTranslator;

    @Override
    public TableData update(TableProperties properties, TableProperties oldProperties,
                            String physicalResourceId) throws AthenaProvisionException {
        if (properties.getName().equals(physicalResourceId)) {
            String queryExecutionId = amazonAthena.startQueryExecution(
                new StartQueryExecutionRequest()
                    .withQueryString(dropTableQueryTranslator.from(oldProperties))
                    .withQueryExecutionContext(new QueryExecutionContext()
                        .withDatabase(oldProperties.getDatabaseName()))
                    .withResultConfiguration(new ResultConfiguration()
                        .withOutputLocation(outputLocation)
                        .withEncryptionConfiguration(
                            EncryptionConfigurationTranslator.from(properties))))
                .getQueryExecutionId();

            amazonAthena.waitQueryExecution(queryExecutionId);
        }

        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(createTableQueryTranslator.from(properties))
                .withQueryExecutionContext(new QueryExecutionContext()
                    .withDatabase(properties.getDatabaseName()))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)
                    .withEncryptionConfiguration(
                        EncryptionConfigurationTranslator.from(properties))))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        return new TableData()
            .withPhysicalResourceId(properties.getName());
    }

}
