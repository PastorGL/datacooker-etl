/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output.functions;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;

public abstract class OutputFunction implements Function2<Integer, Iterator<Tuple2<Object, DataRecord<?>>>, Iterator<Void>> {
    protected final String sub;
    protected final String outputPath;
    protected final HadoopStorage.Codec codec;
    protected final String hadoopConf;
    protected final String[] columns;

    public OutputFunction(String sub, String outputPath, HadoopStorage.Codec codec, String hadoopConf, String[] columns) {
        this.sub = sub;
        this.outputPath = outputPath;
        this.codec = codec;
        this.hadoopConf = hadoopConf;
        this.columns = columns;
    }

    @Override
    public Iterator<Void> call(Integer idx, Iterator<Tuple2<Object, DataRecord<?>>> it) {
        Configuration conf = new Configuration();
        conf.addResource(new ByteArrayInputStream(hadoopConf.getBytes()));

        try {
            writePart(conf, idx, it);
        } catch (Exception e) {
            throw new RuntimeException("Exception while writing records", e);
        }

        return Collections.emptyIterator();
    }

    abstract protected void writePart(Configuration conf, int idx, Iterator<Tuple2<Object, DataRecord<?>>> it) throws Exception;
}
