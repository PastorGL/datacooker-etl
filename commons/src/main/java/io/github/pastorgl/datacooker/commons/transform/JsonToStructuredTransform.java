/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class JsonToStructuredTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("jsonToStructured", StreamType.PlainText, StreamType.Structured,
                "Transform JSON fragments to Structured records",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> new DataStream(StreamType.Structured, ((JavaRDD<Object>) ds.get()).mapPartitions(it -> {
            List<Structured> ret = new ArrayList<>();

            ObjectMapper om = new ObjectMapper();
            om.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
            while (it.hasNext()) {
                ret.add(new Structured(om.readValue(String.valueOf(it.next()), Object.class)));
            }
            return ret.iterator();
        }), newColumns);
    }
}
