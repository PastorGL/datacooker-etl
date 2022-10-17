/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operations;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class CountByKeyOperation extends Operation {
    static final String GEN_COUNT = "_count";

    @Override
    public OperationMeta meta() {
        return new OperationMeta("countByKey", "Count values under the same key in a given KeyValue DataStream." +
                " Output is key to Long count KeyValue DataStream",

                new PositionalStreamsMetaBuilder()
                        .input("KeyValue DataStream to count values under each unique key",
                                new StreamType[]{StreamType.KeyValue}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .output("KeyValue DataStream with unique keys and count of values of input DataStream under each",
                                new StreamType[]{StreamType.KeyValue}, Origin.GENERATED, null
                        )
                        .generated(GEN_COUNT, "Count of key appearances in the source DataStream")
                        .build()
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, DataStream> execute() {
        final List<String> indices = Collections.singletonList(GEN_COUNT);

        JavaPairRDD<Text, Columnar> count = ((JavaPairRDD<Text, Object>) inputStreams.getValue(0).get())
                .mapToPair(t -> new Tuple2<>(t._1, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(t -> new Tuple2<>(t._1, new Columnar(indices, new Object[]{t._2})));

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Columnar, count, Collections.singletonMap(OBJLVL_VALUE, indices)));
    }
}
