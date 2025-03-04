/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operations;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.TransformerOperation;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class CountByKeyOperation extends TransformerOperation {
    static final String GEN_COUNT = "_count";

    @Override
    public PluggableMeta initMeta() {
        return new PluggableMetaBuilder("countByKey", "Count values under the same key in all given DataStreams")
                .operation()
                .input("Source KeyValue DataStream", StreamType.EVERY)
                .output("KeyValue DataStream with unique source keys", StreamType.COLUMNAR, StreamOrigin.GENERATED, null)
                .generated(GEN_COUNT, "Count of values under each key in the source DataStream")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        final List<String> indices = Collections.singletonList(GEN_COUNT);

        return (ds, name) -> {
            JavaPairRDD<Object, DataRecord<?>> count = ds.rdd()
                    .mapToPair(t -> new Tuple2<>(t._1, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair(t -> new Tuple2<>(t._1, new Columnar(indices, new Object[]{t._2})));

            return new DataStreamBuilder(name, Collections.singletonMap(VALUE, indices))
                    .generated(meta.verb, StreamType.Columnar, ds)
                    .build(count);
        };
    }
}
