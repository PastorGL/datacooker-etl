/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class CountByKeyOperation extends Operation {
    static final String GEN_COUNT = "_count";

    @Override
    public OperationMeta meta() {
        return new OperationMeta("countByKey", "Count values under the same key in all given DataStreams",

                new PositionalStreamsMetaBuilder()
                        .input("Source KeyValue DataStream",
                                StreamType.EVERY
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .output("KeyValue DataStream with unique source keys",
                                StreamType.COLUMNAR, StreamOrigin.GENERATED, null
                        )
                        .generated(GEN_COUNT, "Count of values under each key in the source DataStream")
                        .build()
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final List<String> indices = Collections.singletonList(GEN_COUNT);

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);
            JavaPairRDD<Object, DataRecord<?>> count = input.rdd()
                    .mapToPair(t -> new Tuple2<>(t._1, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair(t -> new Tuple2<>(t._1, new Columnar(indices, new Object[]{t._2})));

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), Collections.singletonMap(VALUE, indices))
                    .generated(meta.verb, StreamType.Columnar, input)
                    .build(count)
            );
        }

        return outputs;
    }
}
