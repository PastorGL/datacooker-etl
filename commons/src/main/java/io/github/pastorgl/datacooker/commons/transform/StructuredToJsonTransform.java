/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class StructuredToJsonTransform extends Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("structuredToJson", StreamType.Structured, StreamType.PlainText,
                "Transform Structured records to JSON fragment string file",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> new DataStream(StreamType.PlainText, ds.rdd
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                    ObjectMapper om = new ObjectMapper();
                    while (it.hasNext()) {
                        Tuple2<Object, Record<?>> t = it.next();

                        ret.add(new Tuple2<>(t._1, new PlainText(om.writeValueAsString(t._2))));
                    }
                    return ret.iterator();
                }, true), newColumns);
    }
}
