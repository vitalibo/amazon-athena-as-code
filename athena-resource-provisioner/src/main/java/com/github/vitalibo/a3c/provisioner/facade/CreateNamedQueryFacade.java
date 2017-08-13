package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.CreateNamedQueryRequest;
import com.amazonaws.services.athena.model.CreateNamedQueryResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryData;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import lombok.RequiredArgsConstructor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class CreateNamedQueryFacade implements CreateFacade<NamedQueryProperties, NamedQueryData> {

    private final Collection<Consumer<NamedQueryProperties>> rules;

    private final AmazonAthena amazonAthena;
    private final AmazonS3 amazonS3;

    @Override
    public NamedQueryData create(NamedQueryProperties properties) throws AthenaResourceProvisionException {
        rules.forEach(rule -> rule.accept(properties));

        CreateNamedQueryResult result = amazonAthena.createNamedQuery(new CreateNamedQueryRequest()
            .withDatabase(properties.getDatabase())
            .withQueryString(asQueryString(properties.getQuery()))
            .withDescription(properties.getDescription())
            .withName(properties.getName()));

        NamedQueryData data = new NamedQueryData();
        data.setQueryId(result.getNamedQueryId());
        data.setPhysicalResourceId(result.getNamedQueryId());
        return data;
    }

    private String asQueryString(NamedQueryProperties.Query query) {
        String queryString = query.getQueryString();
        if (!StringUtils.isNullOrEmpty(queryString)) {
            return queryString;
        }

        S3Object object = amazonS3.getObject(
            new GetObjectRequest(query.getS3Bucket(), query.getS3Key()));

        return new BufferedReader(new InputStreamReader(object.getObjectContent()))
            .lines()
            .collect(Collectors.joining(System.lineSeparator()));
    }

}
