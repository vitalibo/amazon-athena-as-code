package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.DatabaseData;
import com.github.vitalibo.a3c.provisioner.model.DatabaseProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import com.github.vitalibo.a3c.provisioner.util.Rules;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateDatabaseFacadeTest {

    @Mock
    private AmazonAthenaSync mockAmazonAthenaSync;
    @Mock
    private QueryStringTranslator<DatabaseProperties> mockQueryStringTranslator;
    @Captor
    private ArgumentCaptor<StartQueryExecutionRequest> captorStartQueryExecutionRequest;
    @Mock
    private Rules<DatabaseProperties> mockRules;

    private CreateDatabaseFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new CreateDatabaseFacade(
            mockRules, mockAmazonAthenaSync, "s3-output-location", mockQueryStringTranslator);
    }

    @Test
    public void testCreate() throws AthenaProvisionException {
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