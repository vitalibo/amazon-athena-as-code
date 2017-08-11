package com.github.vitalibo.a3c.provisioner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClient;
import com.amazonaws.services.athena.model.EncryptionConfiguration;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.util.StringUtils;
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
import java.util.Optional;

public class Factory {

    private static final String AWS_REGION = "AWS_REGION";
    private static final String ATHENA_RESULT_OUTPUT_LOCATION = "ATHENA_RESULT_OUTPUT_LOCATION";
    private static final String ATHENA_RESULT_ENCRYPTION_OPTION = "ATHENA_RESULT_ENCRYPTION_OPTION";
    private static final String ATHENA_RESULT_KMS_KEY = "ATHENA_RESULT_KMS_KEY";

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    private final AmazonS3 amazonS3;
    private final AmazonAthena amazonAthena;
    private final Configuration freemarkerConfiguration;
    private final ResultConfiguration athenaResultConfiguration;

    Factory(Map<String, String> env) {
        final Regions region = Regions.fromName(env.get(AWS_REGION));
        amazonS3 = createAmazonS3(region);
        amazonAthena = createAmazonAthena(region);
        freemarkerConfiguration = createConfiguration();
        athenaResultConfiguration = createResultConfiguration(env);
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
                    athenaResultConfiguration,
                    makeQueryStringTranslator("CreateDatabaseQuery"));
            case ExternalTable:
                return new CreateExternalTableFacade(
                    Collections.emptyList(),
                    new AmazonAthenaSync(amazonAthena),
                    athenaResultConfiguration,
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
                    athenaResultConfiguration,
                    makeQueryStringTranslator("DropDatabaseQuery"));
            case ExternalTable:
                return new DeleteExternalTableFacade(
                    new AmazonAthenaSync(amazonAthena),
                    athenaResultConfiguration,
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
                    athenaResultConfiguration,
                    makeQueryStringTranslator("CreateDatabaseQuery"),
                    makeQueryStringTranslator("UpdateDatabasePropertiesQuery"));
            case ExternalTable:
                return new UpdateExternalTableFacade(
                    Collections.emptyList(),
                    new AmazonAthenaSync(amazonAthena),
                    athenaResultConfiguration,
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

    private static ResultConfiguration createResultConfiguration(Map<String, String> env) {
        return new ResultConfiguration()
            .withOutputLocation(env.get(ATHENA_RESULT_OUTPUT_LOCATION))
            .withEncryptionConfiguration(Optional.ofNullable(
                new EncryptionConfiguration()
                    .withEncryptionOption(env.get(ATHENA_RESULT_ENCRYPTION_OPTION))
                    .withKmsKey(env.get(ATHENA_RESULT_KMS_KEY)))
                .filter(o -> !StringUtils.isNullOrEmpty(o.getEncryptionOption()) && !StringUtils.isNullOrEmpty(o.getKmsKey()))
                .orElse(null));
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
