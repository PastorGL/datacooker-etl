/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.storage.hadoop.output.HadoopOutput;

public abstract class S3DirectOutput extends HadoopOutput {
    protected String accessKey;
    protected String secretKey;

    protected String contentType;
    protected String endpoint;
    protected String region;
    protected String tmpDir;

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

        accessKey = params.get(S3DirectStorage.S3D_ACCESS_KEY);
        secretKey = params.get(S3DirectStorage.S3D_SECRET_KEY);
        endpoint = params.get(S3DirectStorage.S3D_ENDPOINT);
        region = params.get(S3DirectStorage.S3D_REGION);

        contentType = params.get(S3DirectStorage.CONTENT_TYPE);

        tmpDir = params.get("tmp");
    }
}
