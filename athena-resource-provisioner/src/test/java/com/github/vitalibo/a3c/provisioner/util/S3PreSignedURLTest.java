package com.github.vitalibo.a3c.provisioner.util;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class S3PreSignedURLTest {

    @Mock
    private HttpURLConnection mockHttpURLConnection;

    private S3PreSignedURL s3PreSignedURL;

    @BeforeMethod
    public void setUp() throws MalformedURLException {
        MockitoAnnotations.initMocks(this);
        s3PreSignedURL = new S3PreSignedURL(new URL("http://foo.bar"));
    }

    @Test
    public void testUpload() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Mockito.when(mockHttpURLConnection.getOutputStream()).thenReturn(outputStream);
        Mockito.when(mockHttpURLConnection.getResponseCode()).thenReturn(200);

        s3PreSignedURL.upload(mockHttpURLConnection, "response");

        Mockito.verify(mockHttpURLConnection).setDoOutput(true);
        Mockito.verify(mockHttpURLConnection).setRequestMethod("PUT");
        Assert.assertEquals(new String(outputStream.toByteArray()).trim(), "response");
    }

    @Test(expectedExceptions = IOException.class)
    public void testFailUpload() throws IOException {
        Mockito.when(mockHttpURLConnection.getOutputStream()).thenReturn(new ByteArrayOutputStream());
        Mockito.when(mockHttpURLConnection.getResponseCode()).thenReturn(403);

        s3PreSignedURL.upload(mockHttpURLConnection, "response");
    }

}