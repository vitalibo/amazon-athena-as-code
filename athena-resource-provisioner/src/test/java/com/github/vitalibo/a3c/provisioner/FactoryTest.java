package com.github.vitalibo.a3c.provisioner;

import com.github.vitalibo.a3c.provisioner.facade.*;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;
import com.github.vitalibo.a3c.provisioner.util.S3PreSignedURL;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;

public class FactoryTest {

    private Factory factory;

    @BeforeMethod
    public void setUp() {
        factory = new Factory(new HashMap<String, String>() {{
            put("AWS_REGION", "eu-west-1");
        }});
    }

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            sample(ResourceType.NamedQuery,
                CreateNamedQueryFacade.class, DeleteNamedQueryFacade.class, UpdateNamedQueryFacade.class)
        };
    }

    @Test(dataProvider = "samples")
    public void testCreateFacade(ResourceType resourceType, Class<? extends CreateFacade> expected, Object d, Object u) {
        ResourceProviderRequest resourceProviderRequest = new ResourceProviderRequest();
        resourceProviderRequest.setResourceType(resourceType);

        Facade actual = factory.createCreateFacade(resourceProviderRequest);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getClass(), expected);
    }

    @Test(dataProvider = "samples")
    public void testDeleteFacade(ResourceType resourceType, Object c, Class<? extends DeleteFacade> expected, Object u) {
        ResourceProviderRequest resourceProviderRequest = new ResourceProviderRequest();
        resourceProviderRequest.setResourceType(resourceType);

        Facade actual = factory.createDeleteFacade(resourceProviderRequest);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getClass(), expected);
    }

    @Test(dataProvider = "samples")
    public void testUpdateFacade(ResourceType resourceType, Object c, Object d, Class<? extends UpdateFacade> expected) {
        ResourceProviderRequest resourceProviderRequest = new ResourceProviderRequest();
        resourceProviderRequest.setResourceType(resourceType);

        Facade actual = factory.createUpdateFacade(resourceProviderRequest);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getClass(), expected);
    }

    @Test
    public void testCreateS3PreSignedUrl() {
        S3PreSignedURL actual = factory.createS3PreSignedUrl("http://foo.bar");

        Assert.assertNotNull(actual);
    }

    private static Object[] sample(
        ResourceType resourceType, Class<? extends CreateFacade> createFacade,
        Class<? extends DeleteFacade> deleteFacade, Class<? extends UpdateFacade> updateFacade) {
        return new Object[]{resourceType, createFacade, deleteFacade, updateFacade};
    }

}