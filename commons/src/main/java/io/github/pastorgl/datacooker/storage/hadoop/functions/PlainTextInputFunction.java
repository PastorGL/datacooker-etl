package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.InputStream;

public class PlainTextInputFunction extends InputFunction {
    public PlainTextInputFunction(Partitioning partitioning) {
        super(partitioning);
    }

    @Override
    protected RecordStream recordStream(Configuration conf, String inputFile) throws Exception {
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

        return new PlainTextStream(inputStream);
    }
}