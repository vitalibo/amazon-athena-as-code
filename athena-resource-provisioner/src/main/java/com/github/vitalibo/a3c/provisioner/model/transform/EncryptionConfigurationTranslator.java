package com.github.vitalibo.a3c.provisioner.model.transform;

import com.amazonaws.services.athena.model.EncryptionConfiguration;
import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.model.ExternalTableRequest;

public class EncryptionConfigurationTranslator {

    private EncryptionConfigurationTranslator() {
        super();
    }

    public static EncryptionConfiguration from(ExternalTableRequest table) {
        ExternalTableRequest.SerDe o = table.getSerDe();

        if (StringUtils.isNullOrEmpty(o.getEncryptionOption()) || StringUtils.isNullOrEmpty(o.getKmsKey())) {
            return null;
        }

        return new EncryptionConfiguration()
            .withEncryptionOption(o.getEncryptionOption())
            .withKmsKey(o.getKmsKey());
    }

}
