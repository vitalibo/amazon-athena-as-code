package com.github.vitalibo.a3c.provisioner.model.transform;

import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.TestHelper;
import com.github.vitalibo.a3c.provisioner.model.ResourceProperties;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class ResourcePropertiesTranslatorTest {

    @Test(
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = "Unknown resource type.")
    public void testParseUnknownResourceType() throws AthenaProvisionException {
        ResourcePropertiesTranslator translator = ResourcePropertiesTranslator.of(ResourceType.Unknown);

        translator.from(new Object());
    }

    @Test(
        expectedExceptions = AthenaProvisionException.class,
        expectedExceptionsMessageRegExp = "Unrecognized field \"foo\".")
    public void testUnrecognizedField() throws AthenaProvisionException {
        ResourcePropertiesTranslator translator = ResourcePropertiesTranslator.of(ResourceType.NamedQuery);

        translator.from(Collections.singletonMap("foo", "bar"));
    }

    @Test
    public void testTranslate() throws AthenaProvisionException {
        String expected = TestHelper.resourceAsJsonString("/Athena/NamedQuery/Request.json");
        ResourcePropertiesTranslator translator = ResourcePropertiesTranslator.of(ResourceType.NamedQuery);
        Object resourceProperties = Jackson.fromJsonString(expected, Object.class);

        ResourceProperties actual = translator.from(resourceProperties);

        Assert.assertNotNull(actual);
        Assert.assertEquals(Jackson.toJsonString(actual), expected);
    }

}