/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POINT;
import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PointToColumnarTransform extends Transform {
    static final String GEN_CENTER_LAT = "_center_lat";
    static final String GEN_CENTER_LON = "_center_lon";
    static final String GEN_RADIUS = "_radius";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("pointToColumnar", StreamType.Point, StreamType.Columnar,
                "Transform Point DataStream to Columnar",

                null,
                new TransformedStreamMetaBuilder()
                        .genCol(GEN_CENTER_LAT, "Point latitude")
                        .genCol(GEN_CENTER_LON, "Point longitude")
                        .genCol(GEN_RADIUS, "Point radius")
                        .build()
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_VALUE);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_POINT);
            }

            final List<String> _outputColumns = valueColumns;

            return new DataStreamBuilder(ds.name, StreamType.Columnar, Collections.singletonMap(OBJLVL_VALUE, _outputColumns))
                    .transformed(meta.verb, ds)
                    .build(ds.rdd.mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Record<?>> t = it.next();

                            PointEx p = (PointEx) t._2;
                            Columnar out = new Columnar(_outputColumns);
                            for (String col : _outputColumns) {
                                switch (col) {
                                    case GEN_CENTER_LAT: {
                                        out.put(col, p.getY());
                                        break;
                                    }
                                    case GEN_CENTER_LON: {
                                        out.put(col, p.getX());
                                        break;
                                    }
                                    case GEN_RADIUS: {
                                        out.put(col, p.getRadius());
                                        break;
                                    }
                                    default: {
                                        out.put(col, p.asIs(col));
                                    }
                                }

                            }

                            ret.add(new Tuple2<>(t._1, out));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
