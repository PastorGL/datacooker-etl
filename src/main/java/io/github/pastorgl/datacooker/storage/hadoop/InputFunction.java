/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.data.BinRec;
import io.github.pastorgl.datacooker.storage.RecordStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class InputFunction implements Serializable {
    protected final boolean _fromFile;
    protected final String[] _schema;
    protected String[] _columns;
    final protected char _delimiter;

    public InputFunction(boolean fromFile, String[] schema, String[] columns, char delimiter) {
        _fromFile = fromFile;
        _schema = schema;
        _columns = columns;
        _delimiter = delimiter;
    }

    public FlatMapFunction<List<String>, BinRec> build() {
        return (src) -> {
            ArrayList<BinRec> ret = new ArrayList<>();

            Configuration conf = new Configuration();
            try {
                for (String inputFile : src) {
                    RecordStream inputStream = recordStream(conf, inputFile);

                    do {
                        BinRec rec = inputStream.ensureRecord();
                        if (rec == null) {
                            break;
                        } else {
                            ret.add(rec);
                        }
                    } while (true);
                }
            } catch (Exception e) {
                System.err.println("Exception while reading records: " + e.getMessage());
                e.printStackTrace(System.err);
                System.exit(14);
            }

            return ret.iterator();
        };
    }

    protected RecordStream recordStream(Configuration conf, String inputFile) throws Exception {
        String suffix = HadoopStorage.suffix(inputFile);

        if ("parquet".equalsIgnoreCase(suffix)) {
            return new ParquetRecordStream(conf, inputFile, _columns);
        } else {
            Path inputFilePath = new Path(inputFile);
            FileSystem inputFs = inputFilePath.getFileSystem(conf);

            HadoopStorage.Codec codec = HadoopStorage.Codec.lookup(suffix);
            InputStream inputStream = inputFs.open(inputFilePath);
            Class<? extends CompressionCodec> codecClass = codec.codec;
            if (codecClass != null) {
                CompressionCodec cc = codecClass.newInstance();
                ((Configurable) cc).setConf(conf);

                inputStream = cc.createInputStream(inputStream);
            }

            if (_fromFile || (_schema != null) || (_columns != null)) {
                return new DelimitedTextRecordStream(inputStream, _delimiter, _fromFile, _schema, _columns);
            } else {
                return new PlainTextRecordStream(inputStream);
            }
        }
    }
}
