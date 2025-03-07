/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.POINT;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class PointToColumnarTransform extends Transform {
    static final String GEN_CENTER_LAT = "_center_lat";
    static final String GEN_CENTER_LON = "_center_lon";
    static final String GEN_RADIUS = "_radius";
    static final String VERB = "pointToColumnar";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Transform Point DataStream to Columnar")
                .transform().objLvls(VALUE).operation()
                .input(StreamType.POINT, "Input Point DS")
                .output(StreamType.COLUMNAR, "Output Columnar DS")
                .generated(GEN_CENTER_LAT, "Point latitude")
                .generated(GEN_CENTER_LON, "Point longitude")
                .generated(GEN_RADIUS, "Point radius")
                .build();
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = (newColumns != null) ? newColumns.get(VALUE) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(POINT);
            }

            final List<String> _outputColumns = valueColumns;

            return new DataStreamBuilder(ds.name, Collections.singletonMap(VALUE, _outputColumns))
                    .transformed(VERB, StreamType.Columnar, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

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
