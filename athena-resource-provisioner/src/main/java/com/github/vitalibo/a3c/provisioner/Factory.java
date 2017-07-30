package com.github.vitalibo.a3c.provisioner;

import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import lombok.Getter;

import java.util.Map;

public class Factory {

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    Factory(Map<String, String> env) {

    }

    public Facade createCreateFacade(ResourceProviderRequest request) {
        return null;
    }

    public Facade createDeleteFacade(ResourceProviderRequest request) {
        return null;
    }

    public Facade createUpdateFacade(ResourceProviderRequest request) {
        return null;
    }

    public ResponseSender createResponseSender() {
        return null;
    }

}
