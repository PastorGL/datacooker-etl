/**
 * Copyright (C) 2023 Data Cooker team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3DirectStorage {
    public static final String S3D_ACCESS_KEY = "access_key";
    public static final String S3D_SECRET_KEY = "secret_key";
    public static final String S3D_ENDPOINT = "endpoint";
    public static final String S3D_REGION = "region";
    public static final String CONTENT_TYPE = "content_type";
    public static final String PATH_PATTERN = "^s3d://([^/]+)/(.+)";

    public static AmazonS3 get(String endpoint, String region, String accessKey, String secretKey) {
        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (endpoint != null) {
            s3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        }
        if ((accessKey != null) && (secretKey != null)) {
            s3ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }

        return s3ClientBuilder
                .enableForceGlobalBucketAccess()
                .build();
    }
}
