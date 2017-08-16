package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.QueryExecutionStatus;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Delegate;

@RequiredArgsConstructor
public class AmazonAthenaSync implements AmazonAthena {

    @Delegate
    private final AmazonAthena amazonAthena;

    @SneakyThrows(value = InterruptedException.class)
    public void waitQueryExecution(String queryExecutionId) throws AthenaProvisionException {
        QueryExecutionStatus status = amazonAthena.getQueryExecution(
            new GetQueryExecutionRequest()
                .withQueryExecutionId(queryExecutionId))
            .getQueryExecution()
            .getStatus();

        switch (status.getState()) {
            case "SUBMITTED":
            case "RUNNING":
                Thread.sleep(1000);
                waitQueryExecution(queryExecutionId);
                break;
            case "SUCCEEDED":
                break;
            case "FAILED":
            case "CANCELLED":
                throw new AthenaProvisionException(status.getStateChangeReason());
            default:
                throw new IllegalStateException();
        }
    }

}
