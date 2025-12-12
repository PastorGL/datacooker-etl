/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class StructuredToColumnarTransform extends Transformer {
    static final String COLUMN_PREFIX = "column_";
    static final String VERB = "structuredToColumnar";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Transform Structured records to Columnar")
                .transform().reqObjLvls(VALUE)
                .input(StreamType.STRUCTURED, "Input Structured DS")
                .output(StreamType.COLUMNAR, "Output Columnar DS")
                .dynDef(COLUMN_PREFIX, "For each of output columns," +
                        " define JSON query using same syntax as in Structured SELECT", String.class)
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            final List<String> _outputColumns = newColumns.get(VALUE);

            final int cols = _outputColumns.size();
            final String[] props = new String[cols];
            for (int i = 0; i < cols; i++) {
                String col = _outputColumns.get(i);
                props[i] = params.get(COLUMN_PREFIX + col);
            }

            return new DataStreamBuilder(outputName, newColumns)
                    .transformed(VERB, StreamType.Columnar, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            Columnar rec = new Columnar(_outputColumns);
                            for (int i = 0; i < cols; i++) {
                                rec.put(_outputColumns.get(i), t._2.asIs(props[i]));
                            }

                            ret.add(new Tuple2<>(t._1, rec));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
