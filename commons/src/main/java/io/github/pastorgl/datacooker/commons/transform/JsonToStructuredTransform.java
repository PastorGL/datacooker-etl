/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class JsonToStructuredTransform extends Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("jsonToStructured", StreamType.PlainText, StreamType.Structured,
                "Transform JSON fragments to Structured records. Does not preserve partitioning",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> new DataStreamBuilder(ds.name, StreamType.Structured, newColumns)
                .transformed(meta.verb, ds)
                .build(ds.rdd.mapPartitionsToPair(it -> {
                    List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                    ObjectMapper om = new ObjectMapper();
                    om.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
                    while (it.hasNext()) {
                        Tuple2<Object, DataRecord<?>> next = it.next();
                        ret.add(new Tuple2<>(next._1, new Structured(om.readValue(String.valueOf(next._2), Object.class))));
                    }

                    return ret.iterator();
                }));
    }
}
