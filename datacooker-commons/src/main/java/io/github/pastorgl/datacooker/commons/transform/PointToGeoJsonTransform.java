/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import org.wololo.geojson.Feature;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.POINT;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class PointToGeoJsonTransform extends Transform {
    static final String GEN_CENTER_LAT = "_center_lat";
    static final String GEN_CENTER_LON = "_center_lon";
    static final String GEN_RADIUS = "_radius";
    static final String VERB = "pointToGeoJson";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Take a Point DataStream and produce a Plain Text DataStream with GeoJSON fragments")
                .transform(true).objLvls(VALUE).operation()
                .input(StreamType.POINT, "Input Point DS")
                .output(StreamType.PLAIN_TEXT, "Output GeoJson DS")
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

            return new DataStreamBuilder(ds.name, null)
                    .transformed(VERB, StreamType.PlainText, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            PointEx p = (PointEx) t._2;
                            Map<String, Object> featureProps = new HashMap<>();
                            for (String col : _outputColumns) {
                                switch (col) {
                                    case GEN_CENTER_LAT: {
                                        featureProps.put(col, p.getY());
                                        break;
                                    }
                                    case GEN_CENTER_LON: {
                                        featureProps.put(col, p.getX());
                                        break;
                                    }
                                    case GEN_RADIUS: {
                                        featureProps.put(col, p.getRadius());
                                        break;
                                    }
                                    default: {
                                        featureProps.put(col, p.asIs(col));
                                    }
                                }
                            }

                            ret.add(new Tuple2<>(t._1, new PlainText(new Feature(new org.wololo.geojson.Point(new double[]{p.getX(), p.getY()}), featureProps).toString())));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
