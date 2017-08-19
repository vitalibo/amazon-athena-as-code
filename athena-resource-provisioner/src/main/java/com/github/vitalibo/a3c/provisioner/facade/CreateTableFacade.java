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
public class CreateTableFacade implements CreateFacade<TableProperties, TableData> {

    @Delegate
    private final Rules<TableProperties> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<TableProperties> createTableQueryTranslator;

    @Override
    public TableData create(TableProperties properties) throws AthenaProvisionException {
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
