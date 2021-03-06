package com.github.vitalibo.a3c.provisioner.model.transform;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.model.ResourceProperties;
import com.github.vitalibo.a3c.provisioner.model.ResourceType;
import com.github.vitalibo.a3c.provisioner.util.Jackson;
import lombok.RequiredArgsConstructor;

import static com.github.vitalibo.a3c.provisioner.model.ResourceType.Unknown;

@RequiredArgsConstructor(staticName = "of")
public class ResourcePropertiesTranslator {

    private final ResourceType resourceType;

    @SuppressWarnings("unchecked")
    public <T extends ResourceProperties> T from(Object resourceProperties) throws AthenaProvisionException {
        if (Unknown.equals(resourceType)) {
            throw new AthenaProvisionException("Unknown resource type.");
        }

        try {
            return (T) Jackson.convertValue(resourceProperties, resourceType.getTypeClass());
        } catch (IllegalArgumentException e) {
            if (e.getCause() instanceof UnrecognizedPropertyException) {
                throw new AthenaProvisionException(
                    "Unrecognized field \"" + ((UnrecognizedPropertyException) e.getCause()).getPropertyName() + "\".", e.getCause());
            }

            throw e;
        }
    }

}
