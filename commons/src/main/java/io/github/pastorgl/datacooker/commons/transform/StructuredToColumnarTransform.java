/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class StructuredToColumnarTransform implements Transform {
    static final String COLUMN_PREFIX = "column_";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("structuredToColumnar", StreamType.Structured, StreamType.Columnar,
                "Transform Structured records to Columnar records",

                new DefinitionMetaBuilder()
                        .dynDef(COLUMN_PREFIX, "For each of output columns," +
                                " define JSON query using same syntax as in Structured SELECT", String.class)
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final List<String> _outputColumns = newColumns.get(OBJLVL_VALUE);

            final int cols = _outputColumns.size();
            final String[] props = new String[cols];
            for (int i = 0; i < cols; i++) {
                String col = _outputColumns.get(i);
                props[i] = params.get(COLUMN_PREFIX + col);
            }

            return new DataStream(StreamType.Columnar, ((JavaRDD<Object>) ds.get()).mapPartitions(it -> {
                List<Columnar> ret = new ArrayList<>();

                ObjectMapper om = new ObjectMapper();
                while (it.hasNext()) {
                    Structured line = (Structured) it.next();

                    Columnar rec = new Columnar(_outputColumns);
                    for (int i = 0; i < cols; i++) {
                        rec.put(_outputColumns.get(i), line.asIs(props[i]));
                    }

                    ret.add(rec);
                }

                return ret.iterator();
            }), newColumns);
        };
    }
}
