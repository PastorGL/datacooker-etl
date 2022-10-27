/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class ColumnarToPairTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToPair", StreamType.Columnar, StreamType.KeyValue,
                "Transform Columnar DataStream to KeyValue",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_VALUE);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_VALUE);
            }

            List<String> _outputColumns = valueColumns;

            return new DataStream(StreamType.KeyValue, ((JavaPairRDD<String, Columnar>) ds.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, Columnar>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<String, Columnar> line = it.next();

                            Columnar rec = new Columnar(_outputColumns);
                            for (String col : _outputColumns) {
                                rec.put(col, line._2.asIs(col));
                            }

                            ret.add(new Tuple2<>(line._1, rec));
                        }

                        return ret.iterator();
                    }), Collections.singletonMap(OBJLVL_VALUE, _outputColumns));
        };
    }
}
