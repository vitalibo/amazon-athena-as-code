package com.github.vitalibo.a3c.provisioner.util;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

@RequiredArgsConstructor
public class S3PreSignedURL {

    private final URL responseUrl;

    public S3PreSignedURL(String responseUrl) throws MalformedURLException {
        this(new URL(responseUrl));
    }

    public void upload(String response) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) responseUrl.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(
            connection.getOutputStream());
        out.write(response);
        out.close();

        if (connection.getResponseCode() != 200) {
            throw new IOException("Upload response failed with status: " + connection.getResponseMessage());
        }
    }

}
