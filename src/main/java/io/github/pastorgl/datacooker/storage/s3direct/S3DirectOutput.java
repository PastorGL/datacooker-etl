/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopOutput;

public abstract class S3DirectOutput extends HadoopOutput {
    protected String accessKey;
    protected String secretKey;

    protected String contentType;
    protected String endpoint;
    protected String region;
    protected String tmpDir;

    @Override
    protected void configure() throws InvalidConfigurationException {
        super.configure();

        accessKey = resolver.get(S3DirectStorage.S3D_ACCESS_KEY);
        secretKey = resolver.get(S3DirectStorage.S3D_SECRET_KEY);
        endpoint = resolver.get(S3DirectStorage.S3D_ENDPOINT);
        region = resolver.get(S3DirectStorage.S3D_REGION);

        contentType = resolver.get(S3DirectStorage.CONTENT_TYPE);

        tmpDir = resolver.get("tmp");
    }
}
