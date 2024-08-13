/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct.functions;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.ColumnarParquetOutputFunction;
import io.github.pastorgl.datacooker.storage.s3direct.S3DirectStorage;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import java.io.InputStream;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3DirectParquetOutputFunction extends ColumnarParquetOutputFunction {
    private static final int BUFFER_SIZE = 5 * 1024 * 1024;
    private final String accessKey;
    private final String secretKey;

    private final String contentType;
    private final String endpoint;
    private final String region;
    private final Path _tmp;

    public S3DirectParquetOutputFunction(String _name, String outputPath, HadoopStorage.Codec codec, String[] _columns, String endpoint, String region, String accessKey, String secretKey, String tmpDir, String contentType) {
        super(_name, outputPath, codec, _columns);

        this.endpoint = endpoint;
        this.region = region;
        this.secretKey = secretKey;
        this.accessKey = accessKey;
        this.contentType = contentType;

        this._tmp = new Path(tmpDir);
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
        partName += ".parquet";
        key = key.substring(0, key.lastIndexOf("/"));
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

        Path tmpPath = new Path(_tmp + "/" + RandomStringUtils.randomAlphanumeric(16) + "/" + partName);

        writeToParquetFile(conf, it, tmpPath);

        FileSystem tmpFs = tmpPath.getFileSystem(conf);
        InputStream inputStream = tmpFs.open(tmpPath, BUFFER_SIZE);

        int len;
        for (byte[] buffer = new byte[BUFFER_SIZE]; (len = inputStream.read(buffer)) > 0; ) {
            outputStream.write(buffer, 0, len);
        }
        outputStream.close();
        tmpFs.delete(tmpPath, false);
    }
}
