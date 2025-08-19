/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.ParquetColumnarInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class ParquetColumnarInput extends HadoopInput {
    static final String VERB = "parquetColumnar";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports Parquet files (non-splittable), optionally compressed")
                .inputAdapter(new String[]{"hdfs:///path/to/input/with/glob/**/*.snappy.parquet", "file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.parquet"}, true)
                .objLvls(VALUE)
                .output(StreamType.COLUMNAR, "Columnar DS")
                .build();
    }

    @Override
    protected DataStream callForFiles(String name, List<List<String>> partNum) {
        String[] dsColumns = (requestedColumns.get(VALUE) == null) ? null : requestedColumns.get(VALUE).toArray(new String[0]);

        String confXml = "";
        try {
            StringWriter sw = new StringWriter();
            context.hadoopConfiguration().writeXml(sw);
            confXml = sw.toString();
        } catch (IOException ignored) {
        }

        InputFunction inputFunction = new ParquetColumnarInputFunction(dsColumns, confXml, partitioning);
        JavaPairRDD<Object, DataRecord<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        List<String> attrs = Collections.emptyList();
        if (dsColumns != null) {
            attrs = Arrays.asList(dsColumns);
        }
        return new DataStreamBuilder(name, Collections.singletonMap(VALUE, attrs))
                .created(VERB, path, StreamType.Columnar, partitioning.toString())
                .build(rdd);
    }
}
