/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class StructuredToJsonTransform extends Transformer {

    static final String VERB = "structuredToJson";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Transform Structured records to JSON fragment string file")
                .transform(true).operation()
                .input(StreamType.STRUCTURED, "Input Structured DS")
                .output(StreamType.PLAIN_TEXT, "Output JSON DS")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> new DataStreamBuilder(outputName, null)
                .transformed(VERB, StreamType.PlainText, ds)
                .build(ds.rdd().mapPartitionsToPair(it -> {
                    List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                    ObjectMapper om = new ObjectMapper();
                    while (it.hasNext()) {
                        Tuple2<Object, DataRecord<?>> t = it.next();

                        ret.add(new Tuple2<>(t._1, new PlainText(om.writeValueAsString(t._2))));
                    }
                    return ret.iterator();
                }, true));
    }
}
