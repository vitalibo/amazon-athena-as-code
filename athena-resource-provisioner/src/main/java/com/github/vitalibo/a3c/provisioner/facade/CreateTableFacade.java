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
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class CreateTableFacade implements CreateFacade<TableProperties, TableData> {

    private final Collection<Consumer<TableProperties>> rules;

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<TableProperties> createTableQueryTranslator;

    @Override
    public TableData create(TableProperties properties) throws AthenaProvisionException {
        rules.forEach(rule -> rule.accept(properties));

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

        TableData data = new TableData();
        data.setPhysicalResourceId(properties.getName());
        return data;
    }

}
