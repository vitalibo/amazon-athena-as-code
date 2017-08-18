package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.model.DatabaseData;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DeleteDatabaseFacade implements DeleteFacade<DatabaseProperties, DatabaseData> {

    private final AmazonAthenaSync amazonAthena;
    private final String outputLocation;
    private final QueryStringTranslator<DatabaseProperties> dropDatabaseQueryTranslator;

    @Override
    public DatabaseData delete(DatabaseProperties properties, String physicalResourceId) throws AthenaProvisionException {
        String queryExecutionId = amazonAthena.startQueryExecution(
            new StartQueryExecutionRequest()
                .withQueryString(dropDatabaseQueryTranslator.from(properties))
                .withResultConfiguration(new ResultConfiguration()
                    .withOutputLocation(outputLocation)))
            .getQueryExecutionId();

        amazonAthena.waitQueryExecution(queryExecutionId);

        return new DatabaseData()
            .withPhysicalResourceId(physicalResourceId);
    }

}
