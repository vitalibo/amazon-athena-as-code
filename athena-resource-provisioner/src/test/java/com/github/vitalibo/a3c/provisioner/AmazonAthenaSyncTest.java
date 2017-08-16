package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.*;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AmazonAthenaSyncTest {

    @Mock
    private AmazonAthena mockAmazonAthena;
    @Captor
    private ArgumentCaptor<StartQueryExecutionRequest> captorStartQueryExecutionRequest;
    @Captor
    private ArgumentCaptor<GetQueryExecutionRequest> captorGetQueryExecutionRequest;

    private AmazonAthenaSync amazonAthenaSync;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        amazonAthenaSync = new AmazonAthenaSync(mockAmazonAthena);
    }

    @Test
    public void testDelegateStartQueryExecution() {
        Mockito.when(mockAmazonAthena.startQueryExecution(Mockito.any()))
            .thenReturn(new StartQueryExecutionResult()
                .withQueryExecutionId("query-execution-id"));

        StartQueryExecutionResult actual = amazonAthenaSync.startQueryExecution(
            new StartQueryExecutionRequest().withQueryString("sql-query"));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getQueryExecutionId(), "query-execution-id");
        Mockito.verify(mockAmazonAthena).startQueryExecution(captorStartQueryExecutionRequest.capture());
        StartQueryExecutionRequest startQueryExecutionRequest = captorStartQueryExecutionRequest.getValue();
        Assert.assertEquals(startQueryExecutionRequest.getQueryString(), "sql-query");
    }

    @Test
    public void testWaitQueryExecution() throws AthenaProvisionException {
        Mockito.when(mockAmazonAthena.getQueryExecution(Mockito.any()))
            .thenReturn(makeGetQueryExecutionResult(QueryExecutionState.RUNNING),
                makeGetQueryExecutionResult(QueryExecutionState.SUCCEEDED));

        amazonAthenaSync.waitQueryExecution("query-execution-id");

        Mockito.verify(mockAmazonAthena, Mockito.times(2)).getQueryExecution(captorGetQueryExecutionRequest.capture());
        GetQueryExecutionRequest queryExecutionRequest = captorGetQueryExecutionRequest.getValue();
        Assert.assertEquals(queryExecutionRequest.getQueryExecutionId(), "query-execution-id");
    }

    @Test(expectedExceptions = AthenaProvisionException.class)
    public void testFailWaitQueryExecution() throws AthenaProvisionException {
        Mockito.when(mockAmazonAthena.getQueryExecution(Mockito.any()))
            .thenReturn(makeGetQueryExecutionResult(QueryExecutionState.RUNNING),
                makeGetQueryExecutionResult(QueryExecutionState.FAILED));

        amazonAthenaSync.waitQueryExecution("query-execution-id");
    }

    private static GetQueryExecutionResult makeGetQueryExecutionResult(QueryExecutionState state) {
        GetQueryExecutionResult result = new GetQueryExecutionResult();
        QueryExecution queryExecution = new QueryExecution();
        QueryExecutionStatus queryExecutionStatus = new QueryExecutionStatus();
        queryExecutionStatus.setState(state);
        queryExecution.setStatus(queryExecutionStatus);
        result.setQueryExecution(queryExecution);
        return result;
    }

}