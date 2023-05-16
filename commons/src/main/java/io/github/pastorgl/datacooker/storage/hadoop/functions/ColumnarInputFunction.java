package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.hadoop.*;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.InputStream;

public class ColumnarInputFunction extends InputFunction {
    protected final boolean _fromFile;
    protected final String[] _schema;
    protected String[] _columns;
    final protected char _delimiter;

    public ColumnarInputFunction(boolean fromFile, String[] schema, String[] columns, char delimiter, Partitioning partitioning) {
        super(partitioning);

        _fromFile = fromFile;
        _schema = schema;
        _columns = columns;
        _delimiter = delimiter;
    }

    protected RecordStream recordStream(Configuration conf, String inputFile) throws Exception {
        String suffix = HadoopStorage.suffix(inputFile);

        if ("parquet".equalsIgnoreCase(suffix)) {
            return new ParquetColumnarStream(conf, inputFile, _columns);
        } else {
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

            return new DelimitedTextColumnarStream(inputStream, _delimiter, _fromFile, _schema, _columns);
        }
    }
}
