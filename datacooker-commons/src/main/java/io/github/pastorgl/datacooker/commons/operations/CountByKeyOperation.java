/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operations;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class CountByKeyOperation extends Transformer {
    private static final String VERB = "countByKey";
    private static final String GEN_COUNT = "_count";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Count values under the same key in all given DataStreams")
                .operation().transform()
                .input(StreamType.EVERY, "Source KeyValue DataStream")
                .output(StreamType.COLUMNAR, "KeyValue DataStream with unique source keys", StreamOrigin.GENERATED, null)
                .generated(GEN_COUNT, "Count of values under each key in the source DataStream")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, ignore, params) -> {
            final List<String> _indices = Collections.singletonList(GEN_COUNT);

            JavaPairRDD<Object, DataRecord<?>> count = ds.rdd()
                    .mapToPair(t -> new Tuple2<>(t._1, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair(t -> new Tuple2<>(t._1, new Columnar(_indices, new Object[]{t._2})));

            return new DataStreamBuilder(outputName, Collections.singletonMap(VALUE, _indices))
                    .generated(VERB, StreamType.Columnar, ds)
                    .build(count);
        };
    }
}
