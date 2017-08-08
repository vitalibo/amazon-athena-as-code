package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.a3c.provisioner.facade.CreateFacade;
import com.github.vitalibo.a3c.provisioner.facade.DeleteFacade;
import com.github.vitalibo.a3c.provisioner.facade.UpdateFacade;
import com.github.vitalibo.a3c.provisioner.model.ResourceData;
import com.github.vitalibo.a3c.provisioner.model.ResourceProperties;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderResponse;
import com.github.vitalibo.a3c.provisioner.model.Status;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import com.github.vitalibo.a3c.provisioner.util.S3PreSignedURL;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

public class LambdaRequestHandlerTest {

    @Mock
    private Context mockContext;
    @Mock
    private Factory mockFactory;
    @Mock
    private CreateFacade<ResourceProperties, ResourceData> mockCreateFacade;
    @Mock
    private DeleteFacade<ResourceProperties, ResourceData> mockDeleteFacade;
    @Mock
    private UpdateFacade<ResourceProperties, ResourceData> mockUpdateFacade;
    @Mock
    private S3PreSignedURL mockS3PreSignedURL;

    private LambdaRequestHandler lambda;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockFactory.createCreateFacade(Mockito.any())).thenReturn(mockCreateFacade);
        Mockito.when(mockFactory.createDeleteFacade(Mockito.any())).thenReturn(mockDeleteFacade);
        Mockito.when(mockFactory.createUpdateFacade(Mockito.any())).thenReturn(mockUpdateFacade);
        Mockito.when(mockFactory.createS3PreSignedUrl(Mockito.anyString())).thenReturn(mockS3PreSignedURL);
        lambda = new LambdaRequestHandler(mockFactory);
    }

    @Test
    public void testHandleRequest() throws Exception {
        ByteOutputStream outputStream = new ByteOutputStream();
        byte[] resourceProviderRequest = makeResourceProviderRequestJson();
        Mockito.when(mockCreateFacade.process(Mockito.any()))
            .thenReturn(ResourceProviderResponse.builder()
                .status(Status.FAILED).build());
        Mockito.when(mockDeleteFacade.process(Mockito.any()))
            .thenReturn(ResourceProviderResponse.builder()
                .status(Status.FAILED).build());
        Mockito.when(mockUpdateFacade.process(Mockito.any()))
            .thenReturn(ResourceProviderResponse.builder()
                .status(Status.SUCCESS).build());

        lambda.handleRequest(new ByteInputStream(resourceProviderRequest, resourceProviderRequest.length), outputStream, mockContext);

        String actual = new String(outputStream.getBytes());
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.trim(), "{\"Status\":\"SUCCESS\"}");
        Mockito.verify(mockCreateFacade, Mockito.never()).process(Mockito.any());
        Mockito.verify(mockDeleteFacade, Mockito.never()).process(Mockito.any());
        Mockito.verify(mockUpdateFacade).process(Mockito.any());
        Mockito.verify(mockS3PreSignedURL).upload(actual.trim());
    }

    @Test
    public void testFailHandleRequest() throws Exception {
        ByteOutputStream outputStream = new ByteOutputStream();
        byte[] resourceProviderRequest = makeResourceProviderRequestJson();
        Mockito.when(mockCreateFacade.process(Mockito.any()))
            .thenReturn(ResourceProviderResponse.builder()
                .status(Status.FAILED).build());
        Mockito.when(mockDeleteFacade.process(Mockito.any()))
            .thenReturn(ResourceProviderResponse.builder()
                .status(Status.FAILED).build());
        Mockito.when(mockUpdateFacade.process(Mockito.any()))
            .thenThrow(new AthenaResourceProvisionException());

        lambda.handleRequest(new ByteInputStream(resourceProviderRequest, resourceProviderRequest.length), outputStream, mockContext);

        String actual = new String(outputStream.getBytes());
        Assert.assertNotNull(actual);
        Assert.assertTrue(actual.trim().contains("\"Status\":\"FAILED\""));
        Mockito.verify(mockCreateFacade, Mockito.never()).process(Mockito.any());
        Mockito.verify(mockDeleteFacade, Mockito.never()).process(Mockito.any());
        Mockito.verify(mockUpdateFacade).process(Mockito.any());
        Mockito.verify(mockS3PreSignedURL).upload(actual.trim());
    }

    @SuppressWarnings("unchecked")
    private static byte[] makeResourceProviderRequestJson() {
        Map<Object, Object> o = Jackson.fromJsonString(
            TestHelper.resourceAsJsonString("/CloudFormation/Request.json"), Map.class);
        o.put("ResourceType", "Custom::AthenaNamedQuery");
        return Jackson.toJsonString(o).getBytes();
    }

}