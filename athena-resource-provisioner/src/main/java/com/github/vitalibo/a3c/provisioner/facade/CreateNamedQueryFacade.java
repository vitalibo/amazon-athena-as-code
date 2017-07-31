package com.github.vitalibo.a3c.provisioner.facade;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.CreateNamedQueryRequest;
import com.amazonaws.services.athena.model.CreateNamedQueryResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import com.github.vitalibo.a3c.provisioner.AthenaResourceProvisionException;
import com.github.vitalibo.a3c.provisioner.util.Rule;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryRequest;
import com.github.vitalibo.a3c.provisioner.model.NamedQueryResponse;
import lombok.RequiredArgsConstructor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class CreateNamedQueryFacade implements CreateFacade<NamedQueryRequest, NamedQueryResponse> {

    private final Collection<Rule<NamedQueryRequest>> rules;

    private final AmazonAthena amazonAthena;
    private final AmazonS3 amazonS3;

    @Override
    public NamedQueryResponse create(NamedQueryRequest request) throws AthenaResourceProvisionException {
        rules.forEach(rule -> rule.accept(request));

        CreateNamedQueryResult result = amazonAthena.createNamedQuery(new CreateNamedQueryRequest()
            .withDatabase(request.getDatabase())
            .withQueryString(asQueryString(request.getQuery()))
            .withDescription(request.getDescription())
            .withName(request.getName()));

        NamedQueryResponse response = new NamedQueryResponse();
        response.setId(result.getNamedQueryId());
        response.setPhysicalResourceId(result.getNamedQueryId());
        return response;
    }

    private String asQueryString(NamedQueryRequest.Query query) {
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
