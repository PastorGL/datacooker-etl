/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PairToColumnarTransform implements Transform {
    static final String GEN_KEY = "_key";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("pairToColumnar", StreamType.KeyValue, StreamType.Columnar,
                "Origin Pair DataSet to Columnar",

                null,
                new TransformedStreamMetaBuilder()
                        .genCol(GEN_KEY, "Key of the Pair")
                        .build()
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> new DataStream(StreamType.Columnar, ((JavaPairRDD<Text, Columnar>) ds.get())
                .mapPartitions(it -> {
                    List<String> valueColumns = newColumns.get(OBJLVL_VALUE);
                    if (valueColumns == null) {
                        valueColumns = ds.accessor.attributes(OBJLVL_VALUE);
                    }

                    List<String> _outputColumns = valueColumns;
                    int len = _outputColumns.size();

                    List<Columnar> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Tuple2<Text, Columnar> o = it.next();
                        Columnar line = o._2;

                        Columnar rec = new Columnar(valueColumns);
                        for (int i = 0; i < len; i++) {
                            String key = valueColumns.get(i);

                            rec.put(key, key.equalsIgnoreCase(GEN_KEY) ? String.valueOf(o._1) : String.valueOf(line.asIs(key)));
                        }

                        ret.add(rec);
                    }

                    return ret.iterator();
                }), newColumns);
    }
}
