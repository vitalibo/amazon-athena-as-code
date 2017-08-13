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

public class UpdateFacadeTest {

    @Spy
    private UpdateFacade<ResourceProperties, NamedQueryData> spyCreateFacade;
    @Captor
    private ArgumentCaptor<ResourceProperties> namedQueryRequestCaptor;
    @Captor
    private ArgumentCaptor<ResourceProperties> newNamedQueryRequestCaptor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testProcess() throws AthenaResourceProvisionException {
        Object namedQueryRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), Object.class);
        Object newNamedQueryRequest = makeNewNamedQueryRequest(Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), NamedQueryProperties.class));
        NamedQueryData namedQueryData = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Response.json"), NamedQueryData.class);
        ResourceProviderRequest resourceProviderRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/CloudFormation/Request.json"), ResourceProviderRequest.class);
        resourceProviderRequest.setResourceType(ResourceType.NamedQuery);
        resourceProviderRequest.setResourceProperties(newNamedQueryRequest);
        resourceProviderRequest.setOldResourceProperties(namedQueryRequest);
        Mockito.when(spyCreateFacade.update(
            Mockito.any(ResourceProperties.class), Mockito.any(ResourceProperties.class),
            Mockito.eq(resourceProviderRequest.getPhysicalResourceId()))).thenReturn(namedQueryData);

        ResourceProviderResponse actual = spyCreateFacade.process(resourceProviderRequest);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getStatus(), Status.SUCCESS);
        Assert.assertEquals(actual.getLogicalResourceId(), resourceProviderRequest.getLogicalResourceId());
        Assert.assertEquals(actual.getRequestId(), resourceProviderRequest.getRequestId());
        Assert.assertEquals(actual.getStackId(), resourceProviderRequest.getStackId());
        Assert.assertEquals(actual.getPhysicalResourceId(), namedQueryData.getPhysicalResourceId());
        Assert.assertEquals(actual.getData(), namedQueryData);
        Mockito.verify(spyCreateFacade).update(
            newNamedQueryRequestCaptor.capture(), namedQueryRequestCaptor.capture(),
            Mockito.eq(resourceProviderRequest.getPhysicalResourceId()));
        Assert.assertEquals(namedQueryRequestCaptor.getValue(), Jackson.convertValue(namedQueryRequest, NamedQueryProperties.class));
        Assert.assertEquals(newNamedQueryRequestCaptor.getValue(), Jackson.convertValue(newNamedQueryRequest, NamedQueryProperties.class));
    }


    @Test(expectedExceptions = AthenaResourceProvisionException.class)
    public void testFailTranslation() throws AthenaResourceProvisionException {
        ResourceProviderRequest resourceProviderRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/CloudFormation/Request.json"), ResourceProviderRequest.class);
        resourceProviderRequest.setResourceType(ResourceType.NamedQuery);
        resourceProviderRequest.setResourceProperties(Collections.singletonMap("foo", "bar"));

        spyCreateFacade.process(resourceProviderRequest);
    }

    @Test
    public void testUpdateRollbackInProgressAfterFailTranslation() throws AthenaResourceProvisionException {
        Object namedQueryRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), Object.class);
        ResourceProviderRequest resourceProviderRequest = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/CloudFormation/Request.json"), ResourceProviderRequest.class);
        resourceProviderRequest.setResourceType(ResourceType.NamedQuery);
        resourceProviderRequest.setResourceProperties(namedQueryRequest);
        resourceProviderRequest.setOldResourceProperties(Collections.singletonMap("foo", "bar"));

        ResourceProviderResponse actual = spyCreateFacade.process(resourceProviderRequest);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getStatus(), Status.SUCCESS);
        Assert.assertEquals(actual.getLogicalResourceId(), resourceProviderRequest.getLogicalResourceId());
        Assert.assertEquals(actual.getRequestId(), resourceProviderRequest.getRequestId());
        Assert.assertEquals(actual.getStackId(), resourceProviderRequest.getStackId());
        Assert.assertEquals(actual.getPhysicalResourceId(), resourceProviderRequest.getPhysicalResourceId());
        Assert.assertNull(actual.getData());
        Mockito.verify(spyCreateFacade, Mockito.never()).update(Mockito.any(), Mockito.any(), Mockito.any());
    }

    private static Object makeNewNamedQueryRequest(NamedQueryProperties oldNamedQueryProperties) {
        NamedQueryProperties namedQueryProperties = new NamedQueryProperties();
        namedQueryProperties.setName(oldNamedQueryProperties.getName() + "-new");
        namedQueryProperties.setDescription(oldNamedQueryProperties.getDescription() + "-new");
        namedQueryProperties.setQuery(new NamedQueryProperties.Query());
        namedQueryProperties.getQuery().setQueryString(oldNamedQueryProperties.getQuery().getQueryString() + "-new");
        return Jackson.fromJsonString(
            Jackson.toJsonString(namedQueryProperties), Object.class);
    }

}