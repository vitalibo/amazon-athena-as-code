package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.github.vitalibo.a3c.provisioner.AmazonAthenaSync;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.TableData;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import com.github.vitalibo.a3c.provisioner.util.Rules;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateTableFacadeTest {

    @Mock
    private AmazonAthenaSync mockAmazonAthenaSync;
    @Mock
    private QueryStringTranslator<TableProperties> mockCreateQueryStringTranslator;
    @Mock
    private QueryStringTranslator<TableProperties> mockDropQueryStringTranslator;
    @Captor
    private ArgumentCaptor<StartQueryExecutionRequest> captorStartQueryExecutionRequest;
    @Mock
    private Rules<TableProperties> mockRules;

    private UpdateTableFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new UpdateTableFacade(
            mockRules, mockAmazonAthenaSync, "s3-output-location",
            mockCreateQueryStringTranslator, mockDropQueryStringTranslator);
    }

    @Test
    public void testUpdate() throws AthenaProvisionException {
        TableProperties tableProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Table/Request.json"), TableProperties.class);
        StartQueryExecutionResult startQueryExecutionResult = new StartQueryExecutionResult()
            .withQueryExecutionId("query-execution-id");
        Mockito.when(mockAmazonAthenaSync.startQueryExecution(Mockito.any())).thenReturn(startQueryExecutionResult);
        Mockito.when(mockCreateQueryStringTranslator.from(Mockito.any())).thenReturn("sql-create");
        Mockito.when(mockDropQueryStringTranslator.from(Mockito.any())).thenReturn("sql-drop");

        TableData actual = facade.update(tableProperties, tableProperties, tableProperties.getName());

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), tableProperties.getName());
        Mockito.verify(mockAmazonAthenaSync, Mockito.times(2)).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getAllValues().get(0);
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-drop");
        Assert.assertEquals(startQueryExecutionRequest.getQueryExecutionContext().getDatabase(), "DataBaseName");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        startQueryExecutionRequest = captorStartQueryExecutionRequest.getAllValues().get(1);
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-create");
        Assert.assertEquals(startQueryExecutionRequest.getQueryExecutionContext().getDatabase(), "DataBaseName");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        Mockito.verify(mockAmazonAthenaSync, Mockito.times(2)).waitQueryExecution("query-execution-id");
        Mockito.verify(mockDropQueryStringTranslator).from(Mockito.any());
        Mockito.verify(mockCreateQueryStringTranslator).from(Mockito.any());
    }

    @Test
    public void testUpdateCreate() throws AthenaProvisionException {
        TableProperties tableProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Table/Request.json"), TableProperties.class);
        StartQueryExecutionResult startQueryExecutionResult = new StartQueryExecutionResult()
            .withQueryExecutionId("query-execution-id");
        Mockito.when(mockAmazonAthenaSync.startQueryExecution(Mockito.any())).thenReturn(startQueryExecutionResult);
        Mockito.when(mockCreateQueryStringTranslator.from(Mockito.any())).thenReturn("sql-create");
        Mockito.when(mockDropQueryStringTranslator.from(Mockito.any())).thenReturn("sql-drop");

        TableData actual = facade.update(tableProperties, tableProperties, "old-table-name");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), tableProperties.getName());
        Mockito.verify(mockAmazonAthenaSync).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getValue();
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-create");
        Assert.assertEquals(startQueryExecutionRequest.getQueryExecutionContext().getDatabase(), "DataBaseName");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        Mockito.verify(mockAmazonAthenaSync).waitQueryExecution("query-execution-id");
        Mockito.verify(mockDropQueryStringTranslator, Mockito.never()).from(Mockito.any());
        Mockito.verify(mockCreateQueryStringTranslator).from(Mockito.any());
    }

}