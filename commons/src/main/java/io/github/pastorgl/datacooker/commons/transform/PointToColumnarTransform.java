/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_POINT;
import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PointToColumnarTransform implements Transform {
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

            List<String> _outputColumns = valueColumns;

            return new DataStream(StreamType.Columnar, ((JavaRDD<PointEx>) ds.get())
                    .mapPartitions(it -> {
                        List<Columnar> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            PointEx t = it.next();
                            Map<String, Object> props = (Map) t.getUserData();

                            Columnar out = new Columnar(_outputColumns);
                            for (String col : _outputColumns) {
                                switch (col) {
                                    case GEN_CENTER_LAT: {
                                        out.put(col, t.getY());
                                        break;
                                    }
                                    case GEN_CENTER_LON: {
                                        out.put(col, t.getX());
                                        break;
                                    }
                                    case GEN_RADIUS: {
                                        out.put(col, t.getRadius());
                                        break;
                                    }
                                    default: {
                                        out.put(col, props.get(col));
                                    }
                                }

                            }

                            ret.add(out);
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
