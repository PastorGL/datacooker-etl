/**
 * Copyright (C) 2023 Data Cooker team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct.functions;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.util.IOUtils;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.functions.ColumnarInputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.DelimitedTextColumnarStream;
import io.github.pastorgl.datacooker.storage.hadoop.functions.ParquetColumnarStream;
import io.github.pastorgl.datacooker.storage.hadoop.functions.RecordStream;
import io.github.pastorgl.datacooker.storage.s3direct.S3DirectStorage;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.InputStream;
import java.security.MessageDigest;

public class S3DirectColumnarInputFunction extends ColumnarInputFunction {
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;

    private final String _bucket;
    private final Path _tmp;

    public S3DirectColumnarInputFunction(boolean fromFile, String[] schema, String[] columns, char delimiter, String endpoint, String region, String accessKey, String secretKey, String bucket, String tmp, Partitioning partitioning) {
        super(fromFile, schema, columns, delimiter, partitioning);

        this.endpoint = endpoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;

        this._bucket = bucket;
        this._tmp = new Path(tmp);
    }

    @Override
    protected RecordStream recordStream(Configuration conf, String inputFile) throws Exception {
        String suffix = HadoopStorage.suffix(inputFile);

        AmazonS3 _s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);
        InputStream inputStream = _s3.getObject(_bucket, inputFile).getObjectContent();

        if ("parquet".equalsIgnoreCase(suffix)) {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            String pathHash = Hex.encodeHexString(md5.digest((inputFile).getBytes()));

            Path localPath = new Path(_tmp, pathHash);

            FileSystem tmpFs = localPath.getFileSystem(conf);
            if (!tmpFs.exists(localPath)) {
                FSDataOutputStream fso = tmpFs.create(localPath, false);

                IOUtils.copy(inputStream, fso);
                fso.close();
                tmpFs.deleteOnExit(localPath);
            }

            return new ParquetColumnarStream(conf, localPath.toString(), _columns);
        } else {
            HadoopStorage.Codec codec = HadoopStorage.Codec.lookup(suffix);

            Class<? extends CompressionCodec> codecClass = codec.codec;
            if (codecClass != null) {
                CompressionCodec cc = codecClass.getDeclaredConstructor().newInstance();
                ((Configurable) cc).setConf(conf);

                inputStream = cc.createInputStream(inputStream);
            }

            return new DelimitedTextColumnarStream(inputStream, _delimiter, _fromFile, _schema, _columns);
        }
    }
}
