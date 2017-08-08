package com.github.vitalibo.a3c.provisioner.util;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

@RequiredArgsConstructor
public class S3PreSignedURL {

    private final URL responseUrl;

    public void upload(String response) throws IOException {
        upload((HttpURLConnection) responseUrl.openConnection(), response);
    }

    void upload(HttpURLConnection connection, String response) throws IOException {
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
