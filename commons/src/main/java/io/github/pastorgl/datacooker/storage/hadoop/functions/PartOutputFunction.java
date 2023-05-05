/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;

public abstract class PartOutputFunction implements Function2<Integer, Iterator<Tuple2<Object, Record<?>>>, Iterator<Void>> {
    protected final String sub;
    protected final String outputPath;
    protected final HadoopStorage.Codec codec;

    public PartOutputFunction(String sub, String outputPath, HadoopStorage.Codec codec) {
        this.sub = sub;
        this.outputPath = outputPath;
        this.codec = codec;
    }

    @Override
    public Iterator<Void> call(Integer idx, Iterator<Tuple2<Object, Record<?>>> it) {
        Configuration conf = new Configuration();

        try {
            writePart(conf, idx, it);
        } catch (Exception e) {
            System.err.println("Exception while writing records: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(15);
        }

        return Collections.emptyIterator();
    }

    abstract protected void writePart(Configuration conf, int idx, Iterator<Tuple2<Object, Record<?>>> it) throws Exception;
}
