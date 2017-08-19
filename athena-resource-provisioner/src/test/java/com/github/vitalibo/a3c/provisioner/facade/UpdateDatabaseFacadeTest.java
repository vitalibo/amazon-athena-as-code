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

public class UpdateDatabaseFacadeTest {

    @Mock
    private AmazonAthenaSync mockAmazonAthenaSync;
    @Mock
    private QueryStringTranslator<DatabaseProperties> mockCreateQueryStringTranslator;
    @Mock
    private QueryStringTranslator<DatabaseProperties> mockUpdateQueryStringTranslator;
    @Captor
    private ArgumentCaptor<StartQueryExecutionRequest> captorStartQueryExecutionRequest;
    @Mock
    private Rules<DatabaseProperties> mockRules;

    private UpdateDatabaseFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new UpdateDatabaseFacade(
            mockRules, mockAmazonAthenaSync, "s3-output-location",
            mockCreateQueryStringTranslator, mockUpdateQueryStringTranslator);
    }

    @Test
    public void testUpdateCreateStrategy() throws AthenaProvisionException {
        DatabaseProperties databaseProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Database/Request.json"), DatabaseProperties.class);
        Mockito.when(mockAmazonAthenaSync.startQueryExecution(Mockito.any()))
            .thenReturn(new StartQueryExecutionResult().withQueryExecutionId("query-execution-id"));
        Mockito.when(mockCreateQueryStringTranslator.from(Mockito.any())).thenReturn("sql-create");
        Mockito.when(mockUpdateQueryStringTranslator.from(Mockito.any())).thenReturn("sql-update");

        DatabaseData actual = facade.update(
            databaseProperties, databaseProperties, "physical-resource-id");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), databaseProperties.getName());
        Mockito.verify(mockAmazonAthenaSync).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getValue();
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-create");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        Mockito.verify(mockAmazonAthenaSync).waitQueryExecution("query-execution-id");
        Mockito.verify(mockCreateQueryStringTranslator).from(databaseProperties);
        Mockito.verify(mockUpdateQueryStringTranslator, Mockito.never()).from(databaseProperties);
    }

    @Test
    public void testUpdateUpdateStrategy() throws AthenaProvisionException {
        DatabaseProperties databaseProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Database/Request.json"), DatabaseProperties.class);
        Mockito.when(mockAmazonAthenaSync.startQueryExecution(Mockito.any()))
            .thenReturn(new StartQueryExecutionResult().withQueryExecutionId("query-execution-id"));
        Mockito.when(mockCreateQueryStringTranslator.from(Mockito.any())).thenReturn("sql-create");
        Mockito.when(mockUpdateQueryStringTranslator.from(Mockito.any())).thenReturn("sql-update");

        DatabaseData actual = facade.update(
            databaseProperties, databaseProperties, databaseProperties.getName());

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), databaseProperties.getName());
        Mockito.verify(mockAmazonAthenaSync).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getValue();
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-update");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        Mockito.verify(mockAmazonAthenaSync).waitQueryExecution("query-execution-id");
        Mockito.verify(mockCreateQueryStringTranslator, Mockito.never()).from(databaseProperties);
        Mockito.verify(mockUpdateQueryStringTranslator).from(databaseProperties);
    }

}