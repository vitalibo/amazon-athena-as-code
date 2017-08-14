package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.DatabaseData;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class CreateDatabaseFacadeTest {

    @Mock
    private AmazonAthenaSync mockAmazonAthenaSync;
    @Mock
    private QueryStringTranslator<DatabaseProperties> mockQueryStringTranslator;
    @Captor
    private ArgumentCaptor<StartQueryExecutionRequest> captorStartQueryExecutionRequest;

    private CreateDatabaseFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new CreateDatabaseFacade(
            Collections.emptyList(), mockAmazonAthenaSync, "s3-output-location", mockQueryStringTranslator);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testFailValidation() throws AthenaResourceProvisionException {
        facade = new CreateDatabaseFacade(Collections.singleton(o -> {
            throw new RuntimeException();
        }), mockAmazonAthenaSync, "s3-output-location", mockQueryStringTranslator);

        facade.create(new DatabaseProperties());
    }

    @Test
    public void testCreate() throws AthenaResourceProvisionException {
        DatabaseProperties databaseProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Database/Request.json"), DatabaseProperties.class);
        StartQueryExecutionResult startQueryExecutionResult = new StartQueryExecutionResult()
            .withQueryExecutionId("query-execution-id");
        Mockito.when(mockAmazonAthenaSync.startQueryExecution(Mockito.any())).thenReturn(startQueryExecutionResult);
        Mockito.when(mockQueryStringTranslator.from(Mockito.any())).thenReturn("sql-query");

        DatabaseData actual = facade.create(databaseProperties);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), databaseProperties.getName());
        Mockito.verify(mockAmazonAthenaSync).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getValue();
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-query");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        Mockito.verify(mockAmazonAthenaSync).waitQueryExecution("query-execution-id");
        Mockito.verify(mockQueryStringTranslator).from(databaseProperties);
    }

}