package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.github.vitalibo.a3c.provisioner.facade.*;
import com.github.vitalibo.a3c.provisioner.model.ResourceProviderRequest;
import com.github.vitalibo.a3c.provisioner.model.transform.QueryStringTranslator;
import com.github.vitalibo.a3c.provisioner.util.S3PreSignedURL;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import lombok.Getter;
import lombok.SneakyThrows;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class Factory {

    private static final String AWS_REGION = "AWS_REGION";
    private static final String ATHENA_RESULT_OUTPUT_LOCATION = "ATHENA_RESULT_OUTPUT_LOCATION";

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    private final AmazonS3 amazonS3;
    private final AmazonAthena amazonAthena;
    private final Configuration freemarkerConfiguration;
    private final String outputLocation;

    Factory(Map<String, String> env) {
        final Regions region = Regions.fromName(env.get(AWS_REGION));
        this.amazonS3 = createAmazonS3(region);
        this.amazonAthena = createAmazonAthena(region);
        this.freemarkerConfiguration = createConfiguration();
        this.outputLocation = env.get(ATHENA_RESULT_OUTPUT_LOCATION);
    }

    public Facade createCreateFacade(ResourceProviderRequest request) {
        switch (request.getResourceType()) {
            case NamedQuery:
                return new CreateNamedQueryFacade(
                    Collections.emptyList(),
                    amazonAthena,
                    amazonS3);
            case Database:
                return new CreateDatabaseFacade(
                    Collections.emptyList(),
                    new AmazonAthenaSync(amazonAthena),
                    outputLocation,
                    makeQueryStringTranslator("CreateDatabaseQuery"));
            case ExternalTable:
                return new CreateExternalTableFacade(
                    Collections.emptyList(),
                    new AmazonAthenaSync(amazonAthena),
                    outputLocation,
                    makeQueryStringTranslator("CreateTableQuery"));
            default:
                throw new IllegalStateException();
        }
    }

    public Facade createDeleteFacade(ResourceProviderRequest request) {
        switch (request.getResourceType()) {
            case NamedQuery:
                return new DeleteNamedQueryFacade(amazonAthena);
            case Database:
                return new DeleteDatabaseFacade(
                    new AmazonAthenaSync(amazonAthena),
                    outputLocation,
                    makeQueryStringTranslator("DropDatabaseQuery"));
            case ExternalTable:
                return new DeleteExternalTableFacade(
                    new AmazonAthenaSync(amazonAthena),
                    outputLocation,
                    makeQueryStringTranslator("DropTableQuery"));
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
            case Database:
                return new UpdateDatabaseFacade(
                    Collections.emptyList(),
                    new AmazonAthenaSync(amazonAthena),
                    outputLocation,
                    makeQueryStringTranslator("CreateDatabaseQuery"),
                    makeQueryStringTranslator("UpdateDatabasePropertiesQuery"));
            case ExternalTable:
                return new UpdateExternalTableFacade(
                    Collections.emptyList(),
                    new AmazonAthenaSync(amazonAthena),
                    outputLocation,
                    makeQueryStringTranslator("CreateTableQuery"),
                    makeQueryStringTranslator("DropTableQuery"));
            default:
                throw new IllegalStateException();
        }
    }

    @SneakyThrows
    public S3PreSignedURL createS3PreSignedUrl(String responseUrl) {
        return new S3PreSignedURL(new URL(responseUrl));
    }

    @SneakyThrows
    private <T> QueryStringTranslator<T> makeQueryStringTranslator(String resource) {
        return new QueryStringTranslator<>(
            freemarkerConfiguration.getTemplate(
                String.format("query/%s.ftl", resource)));
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

    private static Configuration createConfiguration() {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);
        cfg.setClassForTemplateLoading(Factory.class, "/");
        cfg.setDefaultEncoding(StandardCharsets.UTF_8.name());
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);
        return cfg;
    }

}
