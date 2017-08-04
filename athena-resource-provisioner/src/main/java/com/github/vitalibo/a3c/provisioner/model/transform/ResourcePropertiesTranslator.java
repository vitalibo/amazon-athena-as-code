package com.github.vitalibo.a3c.provisioner.model.transform;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.ResourceProperties;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(staticName = "of")
public class ResourcePropertiesTranslator {

    private final ResourceType resourceType;

    @SuppressWarnings("unchecked")
    public <T extends ResourceProperties> T from(Object resourceProperties) throws AthenaResourceProvisionException {
        try {
            return (T) Jackson.convertValue(resourceProperties, resourceType.getTypeClass());
        } catch (IllegalArgumentException e) {
            if (e.getCause() instanceof UnrecognizedPropertyException) {
                throw new AthenaResourceProvisionException(
                    "Unrecognized field \"" + ((UnrecognizedPropertyException) e.getCause()).getPropertyName() + "\".", e.getCause());
            }

            throw e;
        }
    }

}
