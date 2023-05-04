/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.spark.api.java.JavaPairRDD;

@SuppressWarnings("unused")
public class PassthruTransform extends Transform {
    static final String PART_COUNT = "part_count";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("passthru", StreamType.Passthru, StreamType.Passthru,
                "Repartition a DataStream. Doesn't change it in any other way",

                new DefinitionMetaBuilder()
                        .def(PART_COUNT, "If set, change count of partitions to desired value",
                                Integer.class, null, "By default, don't change number of parts")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            JavaPairRDD<Object, Record<?>> source = ds.rdd;

            int partCount = source.getNumPartitions();
            int reqParts = params.containsKey(PART_COUNT) ? Math.max(params.get(PART_COUNT), 1) : partCount;

            JavaPairRDD<Object, Record<?>> output = source;
            if (reqParts != partCount) {
                output = source.repartition(reqParts);
            }

            return new DataStream(ds.streamType, output, ds.accessor.attributes());
        };
    }
}
