package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.github.vitalibo.a3c.provisioner.facade.CreateNamedQueryFacade;
import com.github.vitalibo.a3c.provisioner.facade.DeleteNamedQueryFacade;
import com.github.vitalibo.a3c.provisioner.facade.UpdateNamedQueryFacade;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;

public class Factory {

    private static final String AWS_REGION = "AWS_REGION";

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    private final AmazonS3 amazonS3;
    private final AmazonAthena amazonAthena;

    Factory(Map<String, String> env) {
        final Regions region = Regions.fromName(env.get(AWS_REGION));
        amazonS3 = createAmazonS3(region);
        amazonAthena = createAmazonAthena(region);
    }

    public Facade createCreateFacade(ResourceProviderRequest request) {
        switch (request.getResourceType()) {
            case NamedQuery:
                return new CreateNamedQueryFacade(
                    Collections.emptyList(),
                    amazonAthena,
                    amazonS3);
            default:
                throw new IllegalStateException();
        }
    }

    public Facade createDeleteFacade(ResourceProviderRequest request) {
        switch (request.getResourceType()) {
            case NamedQuery:
                return new DeleteNamedQueryFacade(amazonAthena);
            default:
                throw new IllegalStateException();
        }
    }

    public Facade createUpdateFacade(ResourceProviderRequest request) {
        switch (request.getResourceType()) {
            case NamedQuery:
                return new UpdateNamedQueryFacade(
                    Collections.emptyList(),
                    amazonAthena,
                    amazonS3);
            default:
                throw new IllegalStateException();
        }
    }

    private static AmazonS3 createAmazonS3(Regions region) {
        return AmazonS3Client.builder()
            .withRegion(region)
            .build();
    }

    private static AmazonAthena createAmazonAthena(Regions region) {
        return AmazonAthenaClient.builder()
            .withRegion(region)
            .build();
    }

}
