/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.hadoop.*;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.InputStream;

public class TextColumnarInputFunction extends InputFunction {
    protected String[] _columns;
    final protected char _delimiter;

    public TextColumnarInputFunction(String[] columns, char delimiter, Partitioning partitioning) {
        super(partitioning);

        _columns = columns;
        _delimiter = delimiter;
    }

    protected RecordInputStream recordStream(Configuration conf, String inputFile) throws Exception {
        String suffix = HadoopStorage.suffix(inputFile);

        Path inputFilePath = new Path(inputFile);
        FileSystem inputFs = inputFilePath.getFileSystem(conf);

        HadoopStorage.Codec codec = HadoopStorage.Codec.lookup(suffix);
        InputStream inputStream = inputFs.open(inputFilePath);
        Class<? extends CompressionCodec> codecClass = codec.codec;
        if (codecClass != null) {
            CompressionCodec cc = codecClass.getDeclaredConstructor().newInstance();
            ((Configurable) cc).setConf(conf);

            inputStream = cc.createInputStream(inputStream);
        }

        return new TextColumnarInputStream(inputStream, _delimiter, _columns);
    }
}
