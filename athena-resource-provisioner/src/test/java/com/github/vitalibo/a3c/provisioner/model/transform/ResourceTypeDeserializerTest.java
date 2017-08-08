package com.github.vitalibo.a3c.provisioner.model.transform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class ResourceTypeDeserializerTest {

    @Mock
    private JsonParser mockJsonParser;
    @Mock
    private DeserializationContext mockDeserializationContext;
    @Mock
    private ObjectCodec mockObjectCodec;
    @Mock
    private TextNode mockTextNode;

    private ResourceTypeDeserializer resourceTypeDeserializer;

    @BeforeMethod
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        resourceTypeDeserializer = new ResourceTypeDeserializer();
        Mockito.when(mockJsonParser.getCodec()).thenReturn(mockObjectCodec);
        Mockito.when(mockObjectCodec.readTree(mockJsonParser)).thenReturn(mockTextNode);
    }

    @Test
    public void testDeserialize() throws IOException {
        Mockito.when(mockTextNode.asText()).thenReturn("Custom::AthenaNamedQuery");

        ResourceType actual = resourceTypeDeserializer.deserialize(mockJsonParser, mockDeserializationContext);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, ResourceType.NamedQuery);
    }

}