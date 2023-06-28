/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct.functions;

import com.amazonaws.services.s3.AmazonS3;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.functions.PlainTextInputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.PlainTextStream;
import io.github.pastorgl.datacooker.storage.hadoop.functions.RecordStream;
import io.github.pastorgl.datacooker.storage.s3direct.S3DirectStorage;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.InputStream;

public class S3DirectTextInputFunction extends PlainTextInputFunction {
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;

    private final String _bucket;

    public S3DirectTextInputFunction(String endpoint, String region, String accessKey, String secretKey, String bucket, Partitioning partitioning) {
        super(partitioning);

        this.endpoint = endpoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;

        this._bucket = bucket;
    }

    @Override
    protected RecordStream recordStream(Configuration conf, String inputFile) throws Exception {
        String suffix = HadoopStorage.suffix(inputFile);

        AmazonS3 _s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);
        InputStream inputStream = _s3.getObject(_bucket, inputFile).getObjectContent();

        HadoopStorage.Codec codec = HadoopStorage.Codec.lookup(suffix);

        Class<? extends CompressionCodec> codecClass = codec.codec;
        if (codecClass != null) {
            CompressionCodec cc = codecClass.getDeclaredConstructor().newInstance();
            ((Configurable) cc).setConf(conf);

            inputStream = cc.createInputStream(inputStream);
        }

        return new PlainTextStream(inputStream);
    }
}
