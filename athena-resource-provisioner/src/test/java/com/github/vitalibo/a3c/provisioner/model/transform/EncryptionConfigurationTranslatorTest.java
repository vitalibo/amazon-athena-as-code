package com.github.vitalibo.a3c.provisioner.model.transform;

import com.amazonaws.services.athena.model.EncryptionConfiguration;
import com.github.vitalibo.a3c.provisioner.model.TableProperties;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EncryptionConfigurationTranslatorTest {

    @Test
    public void testTranslate() {
        TableProperties sample = sample("foo", "bar");

        EncryptionConfiguration actual = EncryptionConfigurationTranslator.from(sample);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getEncryptionOption(), "foo");
        Assert.assertEquals(actual.getKmsKey(), "bar");
    }

    @DataProvider
    public Object[][] sample() {
        return new Object[][]{
            {sample(null, null)},
            {sample("", "")},
            {sample("foo", null)},
            {sample(null, "bar")}
        };
    }

    @Test(dataProvider = "sample")
    public void testNotTranslate(TableProperties sample) {
        EncryptionConfiguration actual = EncryptionConfigurationTranslator.from(sample);

        Assert.assertNull(actual);
    }

    private static TableProperties sample(String encryptionOption, String kmsKey) {
        TableProperties o = new TableProperties();
        TableProperties.SerDe sd = new TableProperties.SerDe();
        sd.setEncryptionOption(encryptionOption);
        sd.setKmsKey(kmsKey);
        o.setSerDe(sd);
        return o;
    }

}