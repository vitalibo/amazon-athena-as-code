package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.DeleteNamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryData;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteNamedQueryFacadeTest {

    @Mock
    private AmazonAthena mockAmazonAthena;
    @Captor
    private ArgumentCaptor<DeleteNamedQueryRequest> captorDeleteNamedQueryRequest;

    private DeleteNamedQueryFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new DeleteNamedQueryFacade(mockAmazonAthena);
    }

    @Test
    public void testDelete() throws AthenaResourceProvisionException {
        NamedQueryProperties namedQueryProperties = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json"), NamedQueryProperties.class);

        NamedQueryData actual = facade.delete(namedQueryProperties, "physical-resource-id");

        Assert.assertNotNull(actual);
        Assert.assertNull(actual.getQueryId());
        Assert.assertEquals(actual.getPhysicalResourceId(), "physical-resource-id");
        Mockito.verify(mockAmazonAthena).deleteNamedQuery(captorDeleteNamedQueryRequest.capture());
        DeleteNamedQueryRequest deleteNamedQueryRequest = captorDeleteNamedQueryRequest.getValue();
        Assert.assertEquals(deleteNamedQueryRequest.getNamedQueryId(), "physical-resource-id");
    }

}