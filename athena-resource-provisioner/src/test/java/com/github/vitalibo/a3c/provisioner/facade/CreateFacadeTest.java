package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.*;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class CreateFacadeTest {

    @Spy
    private CreateFacade<ResourceProperties, NamedQueryResponse> spyCreateFacade;
    @Captor
    private ArgumentCaptor<ResourceProperties> namedQueryRequestCaptor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testProcess() throws AthenaResourceProvisionException {
        Object namedQueryRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), Object.class);
        NamedQueryResponse namedQueryResponse = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Response.json"), NamedQueryResponse.class);
        ResourceProviderRequest resourceProviderRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/CloudFormation/Request.json"), ResourceProviderRequest.class);
        resourceProviderRequest.setResourceType(ResourceType.NamedQuery);
        resourceProviderRequest.setResourceProperties(namedQueryRequest);
        Mockito.when(spyCreateFacade.create(Mockito.any(ResourceProperties.class))).thenReturn(namedQueryResponse);

        ResourceProviderResponse actual = spyCreateFacade.process(resourceProviderRequest);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getStatus(), Status.SUCCESS);
        Assert.assertEquals(actual.getLogicalResourceId(), resourceProviderRequest.getLogicalResourceId());
        Assert.assertEquals(actual.getRequestId(), resourceProviderRequest.getRequestId());
        Assert.assertEquals(actual.getStackId(), resourceProviderRequest.getStackId());
        Assert.assertEquals(actual.getPhysicalResourceId(), namedQueryResponse.getPhysicalResourceId());
        Assert.assertEquals(actual.getData(), namedQueryResponse);
        Mockito.verify(spyCreateFacade).create(namedQueryRequestCaptor.capture());
        Assert.assertEquals(namedQueryRequestCaptor.getValue(), Jackson.convertValue(namedQueryRequest, NamedQueryRequest.class));
    }

    @Test(expectedExceptions = AthenaResourceProvisionException.class)
    public void testFailTranslation() throws AthenaResourceProvisionException {
        ResourceProviderRequest resourceProviderRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/CloudFormation/Request.json"), ResourceProviderRequest.class);
        resourceProviderRequest.setResourceType(ResourceType.NamedQuery);
        resourceProviderRequest.setResourceProperties(Collections.singletonMap("foo", "bar"));

        spyCreateFacade.process(resourceProviderRequest);
    }

}