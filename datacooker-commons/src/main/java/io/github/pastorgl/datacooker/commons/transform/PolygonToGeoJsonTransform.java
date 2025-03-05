/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.wololo.geojson.Feature;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class PolygonToGeoJsonTransform extends Transform {
    @Override
    public PluggableMeta initMeta() {
        return new PluggableMetaBuilder("polygonToGeoJson",
                "Take a Polygon DataStream and produce a Plain Text DataStream with GeoJSON fragments")
                .transform(true).objLvls(VALUE).operation()
                .input(StreamType.POLYGON, "Input Polygon DS")
                .output(StreamType.PLAIN_TEXT, "Output GeoJson DS")
                .build();
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = (newColumns != null) ? newColumns.get(VALUE) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(POLYGON);
            }

            final List<String> _outputColumns = valueColumns;

            return new DataStreamBuilder(ds.name, null)
                    .transformed(meta.verb, StreamType.PlainText, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        Function<Coordinate[], double[][]> convert = (Coordinate[] coordinates) -> {
                            double[][] array = new double[coordinates.length][];
                            for (int i = 0; i < coordinates.length; i++) {
                                array[i] = new double[]{coordinates[i].x, coordinates[i].y};
                            }
                            return array;
                        };

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            PolygonEx poly = (PolygonEx) t._2;
                            int size = poly.getNumInteriorRing() + 1;
                            double[][][] rings = new double[size][][];
                            rings[0] = convert.apply(poly.getExteriorRing().getCoordinates());
                            for (int i = 0; i < size - 1; i++) {
                                rings[i + 1] = convert.apply(poly.getInteriorRingN(i).getCoordinates());
                            }

                            Map<String, Object> featureProps = new HashMap<>();
                            for (String column : _outputColumns) {
                                featureProps.put(column, poly.asIs(column));
                            }

                            ret.add(new Tuple2<>(t._1, new PlainText(new Feature(new org.wololo.geojson.Polygon(rings), featureProps).toString())));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
