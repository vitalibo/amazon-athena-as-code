package com.github.vitalibo.a3c.provisioner;

import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderResponse;

public interface Facade {

    ResourceProviderResponse process(ResourceProviderRequest request) throws AthenaResourceProvisionException;

}
