/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct.functions;

import com.amazonaws.services.s3.AmazonS3;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.s3direct.S3DirectStorage;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.RecordInputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public class S3DirectTextInputFunction extends InputFunction {
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;

    private final String _bucket;

    public S3DirectTextInputFunction(String endpoint, String region, String accessKey, String secretKey, String bucket, String hadoopConf, Partitioning partitioning) {
        super(hadoopConf, partitioning);

        this.endpoint = endpoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;

        this._bucket = bucket;
    }

    @Override
    protected RecordInputStream recordStream(String inputFile) throws Exception {
        String suffix = HadoopStorage.suffix(inputFile);

        AmazonS3 _s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);
        InputStream inputStream = _s3.getObject(_bucket, inputFile).getObjectContent();

        HadoopStorage.Codec codec = HadoopStorage.Codec.lookup(suffix);

        Class<? extends CompressionCodec> codecClass = codec.codec;
        if (codecClass != null) {
            Configuration hadoopConf = new Configuration();
            hadoopConf.addResource(new ByteArrayInputStream(confXml.getBytes()));

            CompressionCodec cc = codecClass.getDeclaredConstructor().newInstance();
            ((Configurable) cc).setConf(hadoopConf);

            inputStream = cc.createInputStream(inputStream);
        }

        return new PlainTextStream(inputStream);
    }
}
