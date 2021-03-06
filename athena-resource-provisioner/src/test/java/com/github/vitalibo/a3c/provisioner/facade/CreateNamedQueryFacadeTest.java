package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.CreateNamedQueryRequest;
import com.amazonaws.services.athena.model.CreateNamedQueryResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryData;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import com.github.vitalibo.a3c.provisioner.util.Rules;
import org.apache.http.client.methods.HttpGet;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;

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
    @Mock
    private Rules<NamedQueryProperties> mockRules;

    private CreateNamedQueryFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new CreateNamedQueryFacade(mockRules, mockAmazonAthena, mockAmazonS3);
    }

    @Test
    public void testCreate() throws AthenaProvisionException {
        NamedQueryProperties namedQueryProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), NamedQueryProperties.class);
        CreateNamedQueryResult createNamedQueryResult = new CreateNamedQueryResult();
        Mockito.when(mockAmazonAthena.createNamedQuery(Mockito.any())).thenReturn(createNamedQueryResult);
        createNamedQueryResult.setNamedQueryId("named-query-id");
        NamedQueryProperties.Query query = namedQueryProperties.getQuery();
        query.setS3Bucket(null);
        query.setS3Key(null);

        NamedQueryData actual = facade.create(namedQueryProperties);

        Assert.assertNotNull(actual);
        Mockito.verify(mockAmazonAthena).createNamedQuery(captorCreateNamedQueryRequest.capture());
        CreateNamedQueryRequest createNamedQueryRequest = captorCreateNamedQueryRequest.getValue();
        Assert.assertEquals(createNamedQueryRequest.getDatabase(), namedQueryProperties.getDatabase());
        Assert.assertEquals(createNamedQueryRequest.getQueryString(), namedQueryProperties.getQuery().getQueryString());
        Assert.assertEquals(createNamedQueryRequest.getDescription(), namedQueryProperties.getDescription());
        Assert.assertEquals(createNamedQueryRequest.getName(), namedQueryProperties.getName());
        Assert.assertEquals(actual.getPhysicalResourceId(), "named-query-id");
        Assert.assertEquals(actual.getNamedQueryId(), "named-query-id");
        Mockito.verify(mockAmazonS3, Mockito.never()).getObject(Mockito.any());
    }

    @Test
    public void testCreateFromS3() throws AthenaProvisionException {
        NamedQueryProperties namedQueryProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), NamedQueryProperties.class);
        CreateNamedQueryResult createNamedQueryResult = new CreateNamedQueryResult();
        Mockito.when(mockAmazonAthena.createNamedQuery(Mockito.any())).thenReturn(createNamedQueryResult);
        createNamedQueryResult.setNamedQueryId("named-query-id");
        NamedQueryProperties.Query query = namedQueryProperties.getQuery();
        query.setQueryString(null);
        Mockito.when(mockAmazonS3.getObject(Mockito.any())).thenReturn(mockS3Object);
        Mockito.when(mockS3Object.getObjectContent()).thenReturn(
            new S3ObjectInputStream(new ByteArrayInputStream("s3-query".getBytes()), new HttpGet()));

        NamedQueryData actual = facade.create(namedQueryProperties);

        Assert.assertNotNull(actual);
        Mockito.verify(mockAmazonAthena).createNamedQuery(captorCreateNamedQueryRequest.capture());
        CreateNamedQueryRequest createNamedQueryRequest = captorCreateNamedQueryRequest.getValue();
        Assert.assertEquals(createNamedQueryRequest.getDatabase(), namedQueryProperties.getDatabase());
        Assert.assertEquals(createNamedQueryRequest.getQueryString(), "s3-query");
        Assert.assertEquals(createNamedQueryRequest.getDescription(), namedQueryProperties.getDescription());
        Assert.assertEquals(createNamedQueryRequest.getName(), namedQueryProperties.getName());
        Assert.assertEquals(actual.getPhysicalResourceId(), "named-query-id");
        Assert.assertEquals(actual.getNamedQueryId(), "named-query-id");
        Mockito.verify(mockAmazonS3).getObject(captorGetObjectRequest.capture());
        GetObjectRequest getObjectRequest = captorGetObjectRequest.getValue();
        Assert.assertEquals(getObjectRequest.getBucketName(), namedQueryProperties.getQuery().getS3Bucket());
        Assert.assertEquals(getObjectRequest.getKey(), namedQueryProperties.getQuery().getS3Key());
    }

}