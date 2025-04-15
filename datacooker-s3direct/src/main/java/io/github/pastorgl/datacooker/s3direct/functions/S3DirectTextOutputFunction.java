/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct.functions;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.HadoopTextOutputFunction;
import io.github.pastorgl.datacooker.s3direct.S3DirectStorage;
import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3DirectTextOutputFunction extends HadoopTextOutputFunction {
    private final String accessKey;
    private final String secretKey;

    private final String contentType;
    private final String endpoint;
    private final String region;

    public S3DirectTextOutputFunction(String _name, String outputPath, HadoopStorage.Codec codec, String hadoopConf, String[] _columns, char _delimiter, String endpoint, String region, String accessKey, String secretKey, String contentType) {
        super(_name, outputPath, codec, hadoopConf, _columns, _delimiter);

        this.endpoint = endpoint;
        this.region = region;
        this.secretKey = secretKey;
        this.accessKey = accessKey;
        this.contentType = contentType;
    }

    @Override
    protected void writePart(Configuration conf, int idx, Iterator<Tuple2<Object, DataRecord<?>>> it) throws Exception {
        Matcher m = Pattern.compile(S3DirectStorage.PATH_PATTERN).matcher(outputPath);
        m.matches();

        final String bucket = m.group(1);
        String key = m.group(2);

        String partName = (sub.isEmpty() ? "" : ("/" + sub)) + "/" + String.format("part-%05d", idx);
        if (codec != HadoopStorage.Codec.NONE) {
            partName += "." + codec.name().toLowerCase();
        }
        key += partName;

        System.out.println("Writing S3 object " + key);

        AmazonS3 _s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);

        StreamTransferManager stm = new StreamTransferManager(bucket, key, _s3) {
            @Override
            public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
                ObjectMetadata om = new ObjectMetadata();
                om.setContentType(contentType);
                request.setObjectMetadata(om);
            }
        };

        MultiPartOutputStream outputStream = stm.numStreams(1)
                .numUploadThreads(1)
                .queueCapacity(1)
                .partSize(15)
                .getMultiPartOutputStreams().get(0);

        writeToTextFile(it, outputStream);
    }
}
