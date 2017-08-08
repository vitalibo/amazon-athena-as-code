package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.CreateNamedQueryRequest;
import com.amazonaws.services.athena.model.CreateNamedQueryResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryResponse;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.apache.http.client.methods.HttpGet;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;

public class CreateNamedQueryFacadeTest {

    @Mock
    private AmazonAthena mockAmazonAthena;
    @Mock
    private AmazonS3 mockAmazonS3;
    @Captor
    private ArgumentCaptor<CreateNamedQueryRequest> captorCreateNamedQueryRequest;
    @Mock
    private S3Object mockS3Object;
    @Captor
    private ArgumentCaptor<GetObjectRequest> captorGetObjectRequest;

    private CreateNamedQueryFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new CreateNamedQueryFacade(Collections.emptyList(), mockAmazonAthena, mockAmazonS3);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testFailValidation() throws AthenaResourceProvisionException {
        facade = new CreateNamedQueryFacade(Collections.singletonList(o -> {
            throw new RuntimeException();
        }), mockAmazonAthena, mockAmazonS3);

        facade.create(new NamedQueryRequest());
    }

    @Test
    public void testCreate() throws AthenaResourceProvisionException {
        NamedQueryRequest namedQueryRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), NamedQueryRequest.class);
        CreateNamedQueryResult createNamedQueryResult = new CreateNamedQueryResult();
        Mockito.when(mockAmazonAthena.createNamedQuery(Mockito.any())).thenReturn(createNamedQueryResult);
        createNamedQueryResult.setNamedQueryId("named-query-id");
        NamedQueryRequest.Query query = namedQueryRequest.getQuery();
        query.setS3Bucket(null);
        query.setS3Key(null);

        NamedQueryResponse actual = facade.create(namedQueryRequest);

        Assert.assertNotNull(actual);
        Mockito.verify(mockAmazonAthena).createNamedQuery(captorCreateNamedQueryRequest.capture());
        CreateNamedQueryRequest createNamedQueryRequest = captorCreateNamedQueryRequest.getValue();
        Assert.assertEquals(createNamedQueryRequest.getDatabase(), namedQueryRequest.getDatabase());
        Assert.assertEquals(createNamedQueryRequest.getQueryString(), namedQueryRequest.getQuery().getQueryString());
        Assert.assertEquals(createNamedQueryRequest.getDescription(), namedQueryRequest.getDescription());
        Assert.assertEquals(createNamedQueryRequest.getName(), namedQueryRequest.getName());
        Assert.assertEquals(actual.getPhysicalResourceId(), "named-query-id");
        Assert.assertEquals(actual.getQueryId(), "named-query-id");
        Mockito.verify(mockAmazonS3, Mockito.never()).getObject(Mockito.any());
    }

    @Test
    public void testCreateFromS3() throws AthenaResourceProvisionException {
        NamedQueryRequest namedQueryRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), NamedQueryRequest.class);
        CreateNamedQueryResult createNamedQueryResult = new CreateNamedQueryResult();
        Mockito.when(mockAmazonAthena.createNamedQuery(Mockito.any())).thenReturn(createNamedQueryResult);
        createNamedQueryResult.setNamedQueryId("named-query-id");
        NamedQueryRequest.Query query = namedQueryRequest.getQuery();
        query.setQueryString(null);
        Mockito.when(mockAmazonS3.getObject(Mockito.any())).thenReturn(mockS3Object);
        Mockito.when(mockS3Object.getObjectContent()).thenReturn(
            new S3ObjectInputStream(new ByteArrayInputStream("s3-query".getBytes()), new HttpGet()));

        NamedQueryResponse actual = facade.create(namedQueryRequest);

        Assert.assertNotNull(actual);
        Mockito.verify(mockAmazonAthena).createNamedQuery(captorCreateNamedQueryRequest.capture());
        CreateNamedQueryRequest createNamedQueryRequest = captorCreateNamedQueryRequest.getValue();
        Assert.assertEquals(createNamedQueryRequest.getDatabase(), namedQueryRequest.getDatabase());
        Assert.assertEquals(createNamedQueryRequest.getQueryString(), "s3-query");
        Assert.assertEquals(createNamedQueryRequest.getDescription(), namedQueryRequest.getDescription());
        Assert.assertEquals(createNamedQueryRequest.getName(), namedQueryRequest.getName());
        Assert.assertEquals(actual.getPhysicalResourceId(), "named-query-id");
        Assert.assertEquals(actual.getQueryId(), "named-query-id");
        Mockito.verify(mockAmazonS3).getObject(captorGetObjectRequest.capture());
        GetObjectRequest getObjectRequest = captorGetObjectRequest.getValue();
        Assert.assertEquals(getObjectRequest.getBucketName(), namedQueryRequest.getQuery().getS3Bucket());
        Assert.assertEquals(getObjectRequest.getKey(), namedQueryRequest.getQuery().getS3Key());
    }

}