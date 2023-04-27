/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class StructuredToJsonTransform implements Transform {
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
        return (ds, newColumns, params) -> new DataStream(StreamType.PlainText, ((JavaRDD<Object>) ds.get()).mapPartitions(it -> {
            List<Text> ret = new ArrayList<>();

            ObjectMapper om = new ObjectMapper();
            while (it.hasNext()) {
                ret.add(new PlainText(om.writeValueAsString(it.next())));
            }
            return ret.iterator();
        }), newColumns);
    }
}
