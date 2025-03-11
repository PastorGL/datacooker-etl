/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
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
public class JsonToStructuredTransform extends Transformer {

    static final String VERB = "jsonToStructured";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Transform JSON fragments to Structured records. Does not preserve partitioning")
                .transform().operation()
                .input(StreamType.PLAIN_TEXT, "Input JSON DS")
                .output(StreamType.STRUCTURED, "Output Structured DS")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> new DataStreamBuilder(outputName, newColumns)
                .transformed(VERB, StreamType.Structured, ds)
                .build(ds.rdd().mapPartitionsToPair(it -> {
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
