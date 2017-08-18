package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.CreateNamedQueryRequest;
import com.amazonaws.services.athena.model.CreateNamedQueryResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.AthenaProvisionException;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryData;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryProperties;
import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@AllArgsConstructor
public class UpdateNamedQueryFacade implements UpdateFacade<NamedQueryProperties, NamedQueryData> {

    private final Collection<Consumer<NamedQueryProperties>> rules;

    private final AmazonAthena amazonAthena;
    private final AmazonS3 amazonS3;

    @Override
    public NamedQueryData update(NamedQueryProperties properties, NamedQueryProperties oldProperties,
                                 String physicalResourceId) throws AthenaProvisionException {
        try {
            rules.forEach(rule -> rule.accept(oldProperties));
        } catch (AthenaProvisionException ignore) {
            // When status UPDATE_ROLLBACK_IN_PROGRESS
            return new NamedQueryData()
                .withPhysicalResourceId(physicalResourceId);
        }
        rules.forEach(rule -> rule.accept(properties));

        CreateNamedQueryResult result = amazonAthena.createNamedQuery(new CreateNamedQueryRequest()
            .withDatabase(properties.getDatabase())
            .withQueryString(asQueryString(properties.getQuery()))
            .withDescription(properties.getDescription())
            .withName(properties.getName()));

        return new NamedQueryData()
            .withNamedQueryId(result.getNamedQueryId())
            .withPhysicalResourceId(result.getNamedQueryId());
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
