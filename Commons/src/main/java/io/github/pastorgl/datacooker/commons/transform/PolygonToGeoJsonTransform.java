/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.wololo.geojson.Feature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_POLYGON;
import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PolygonToGeoJsonTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("polygonToGeoJson", StreamType.Polygon, StreamType.PlainText,
                "Take a Polygon DataStream and produce a Plain Text DataStream with GeoJSON fragments",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_VALUE);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_POLYGON);
            }
            List<String> _outputColumns = valueColumns;

            return new DataStream(StreamType.PlainText, ((JavaRDD<Polygon>) ds.get())
                    .mapPartitions(it -> {
                        List<Text> result = new ArrayList<>();

                        Function<Coordinate[], double[][]> convert = (Coordinate[] coordinates) -> {
                            double[][] array = new double[coordinates.length][];
                            for (int i = 0; i < coordinates.length; i++) {
                                array[i] = new double[]{coordinates[i].x, coordinates[i].y};
                            }
                            return array;
                        };

                        while (it.hasNext()) {
                            Polygon poly = it.next();
                            Map<String, Object> props = (Map) poly.getUserData();

                            int size = poly.getNumInteriorRing() + 1;
                            double[][][] rings = new double[size][][];
                            rings[0] = convert.apply(poly.getExteriorRing().getCoordinates());
                            for (int i = 0; i < size - 1; i++) {
                                rings[i + 1] = convert.apply(poly.getInteriorRingN(i).getCoordinates());
                            }

                            Map<String, Object> featureProps = new HashMap<>();
                            for (String column : _outputColumns) {
                                featureProps.put(column, props.get(column));
                            }

                            result.add(new Text(new Feature(new org.wololo.geojson.Polygon(rings), featureProps).toString()));
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
