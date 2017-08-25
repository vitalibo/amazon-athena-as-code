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

public class CreateTableFacadeTest {

    @Mock
    private AmazonAthenaSync mockAmazonAthenaSync;
    @Mock
    private QueryStringTranslator<TableProperties> mockQueryStringTranslator;
    @Captor
    private ArgumentCaptor<StartQueryExecutionRequest> captorStartQueryExecutionRequest;
    @Mock
    private Rules<TableProperties> mockRules;

    private CreateTableFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new CreateTableFacade(
            mockRules, mockAmazonAthenaSync, "s3-output-location", mockQueryStringTranslator);
    }

    @Test
    public void testCreate() throws AthenaProvisionException {
        TableProperties tableProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/Table/Request.json"), TableProperties.class);
        StartQueryExecutionResult startQueryExecutionResult = new StartQueryExecutionResult()
            .withQueryExecutionId("query-execution-id");
        Mockito.when(mockAmazonAthenaSync.startQueryExecution(Mockito.any())).thenReturn(startQueryExecutionResult);
        Mockito.when(mockQueryStringTranslator.from(Mockito.any())).thenReturn("sql-query");

        TableData actual = facade.create(tableProperties);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), tableProperties.getDatabaseName() + "." + tableProperties.getName());
        Mockito.verify(mockAmazonAthenaSync).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getValue();
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-query");
        Assert.assertEquals(startQueryExecutionRequest.getQueryExecutionContext().getDatabase(), "DataBaseName");
        Assert.assertEquals(startQueryExecutionRequest.getResultConfiguration().getOutputLocation(), "s3-output-location");
        Mockito.verify(mockAmazonAthenaSync).waitQueryExecution("query-execution-id");
        Mockito.verify(mockQueryStringTranslator).from(tableProperties);
    }

}