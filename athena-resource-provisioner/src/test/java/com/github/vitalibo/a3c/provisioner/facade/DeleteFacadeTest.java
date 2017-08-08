package com.github.vitalibo.a3c.provisioner.facade;

import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.*;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteFacadeTest {

    @Spy
    private DeleteFacade<ResourceProperties, NamedQueryResponse> spyCreateFacade;
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
        Mockito.when(spyCreateFacade.delete(Mockito.any(ResourceProperties.class), Mockito.eq(resourceProviderRequest.getPhysicalResourceId())))
            .thenReturn(namedQueryResponse);

        ResourceProviderResponse actual = spyCreateFacade.process(resourceProviderRequest);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getStatus(), Status.SUCCESS);
        Assert.assertEquals(actual.getLogicalResourceId(), resourceProviderRequest.getLogicalResourceId());
        Assert.assertEquals(actual.getRequestId(), resourceProviderRequest.getRequestId());
        Assert.assertEquals(actual.getStackId(), resourceProviderRequest.getStackId());
        Assert.assertEquals(actual.getPhysicalResourceId(), namedQueryResponse.getPhysicalResourceId());
        Assert.assertEquals(actual.getData(), namedQueryResponse);
        Mockito.verify(spyCreateFacade).delete(namedQueryRequestCaptor.capture(), Mockito.eq(resourceProviderRequest.getPhysicalResourceId()));
        Assert.assertEquals(namedQueryRequestCaptor.getValue(), Jackson.convertValue(namedQueryRequest, NamedQueryRequest.class));
    }

}