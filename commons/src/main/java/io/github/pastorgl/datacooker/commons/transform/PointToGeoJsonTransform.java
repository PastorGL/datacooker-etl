/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.wololo.geojson.Feature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POINT;
import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PointToGeoJsonTransform implements Transform {
    static final String GEN_CENTER_LAT = "_center_lat";
    static final String GEN_CENTER_LON = "_center_lon";
    static final String GEN_RADIUS = "_radius";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("pointToGeoJson", StreamType.Point, StreamType.PlainText,
                "Take a Point DataStream and produce a Plain Text DataStream with GeoJSON fragments",

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

            return new DataStream(StreamType.PlainText, ((JavaRDD<PointEx>) ds.get())
                    .mapPartitions(it -> {
                        List<Text> result = new ArrayList<>();

                        while (it.hasNext()) {
                            PointEx point = it.next();
                            Map<String, Object> props = (Map) point.getUserData();

                            Map<String, Object> featureProps = new HashMap<>();
                            for (String col : _outputColumns) {
                                switch (col) {
                                    case GEN_CENTER_LAT: {
                                        featureProps.put(col, point.getY());
                                        break;
                                    }
                                    case GEN_CENTER_LON: {
                                        featureProps.put(col, point.getX());
                                        break;
                                    }
                                    case GEN_RADIUS: {
                                        featureProps.put(col, point.getRadius());
                                        break;
                                    }
                                    default: {
                                        featureProps.put(col, props.get(col));
                                    }
                                }
                            }

                            result.add(new PlainText(new Feature(new org.wololo.geojson.Point(new double[]{point.getX(), point.getY()}), featureProps).toString()));
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
