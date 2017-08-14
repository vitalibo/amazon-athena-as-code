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

public class DeleteDatabaseFacadeTest {

    @Mock
    private AmazonAthenaSync mockAmazonAthenaSync;
    @Mock
    private QueryStringTranslator<DatabaseProperties> mockQueryStringTranslator;
    @Captor
    private ArgumentCaptor<StartQueryExecutionRequest> captorStartQueryExecutionRequest;

    private DeleteDatabaseFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new DeleteDatabaseFacade(
            mockAmazonAthenaSync, "s3-output-location", mockQueryStringTranslator);
    }

    @Test
    public void testDelete() throws AthenaResourceProvisionException {
        DatabaseProperties databaseProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Database/Request.json"), DatabaseProperties.class);
        Mockito.when(mockAmazonAthenaSync.startQueryExecution(Mockito.any()))
            .thenReturn(new StartQueryExecutionResult().withQueryExecutionId("query-execution-id"));
        Mockito.when(mockQueryStringTranslator.from(Mockito.any())).thenReturn("sql-query");

        DatabaseData actual = facade.delete(databaseProperties, "physical-resource-id");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), "physical-resource-id");
        Mockito.verify(mockAmazonAthenaSync).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getValue();
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-query");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        Mockito.verify(mockAmazonAthenaSync).waitQueryExecution("query-execution-id");
        Mockito.verify(mockQueryStringTranslator).from(databaseProperties);
    }

}