/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.*;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_POINT;
import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class ColumnarToPointTransform implements Transform {
    static final String RADIUS_DEFAULT = "radius_default";
    static final String RADIUS_COLUMN = "radius_column";
    static final String LAT_COLUMN = "lat_column";
    static final String LON_COLUMN = "lon_column";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToPoint", StreamType.Columnar, StreamType.Point,
                "Transform Columnar DataStream to Point by setting coordinates and optional radius",

                new DefinitionMetaBuilder()
                        .def(RADIUS_DEFAULT, "If set, generated Points will have this value in the radius parameter",
                                Double.class, Double.NaN, "By default, Point radius attribute is not set")
                        .def(RADIUS_COLUMN, "If set, generated Points will take their radius parameter from the specified column instead",
                                null, "By default, don't set Point radius attribute")
                        .def(LAT_COLUMN, "Point latitude column")
                        .def(LON_COLUMN, "Point longitude column")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get(OBJLVL_POINT);
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes(OBJLVL_VALUE);
            }

            final List<String> _outputColumns = valueColumns;

            final String latColumn = params.get(LAT_COLUMN);
            final String lonColumn = params.get(LON_COLUMN);
            final String radiusColumn = params.get(RADIUS_COLUMN);
            final Double defaultRadius = params.get(RADIUS_DEFAULT);

            if ((latColumn == null) || (lonColumn == null)) {
                throw new InvalidConfigurationException("Parameters lat_column and lon_column are both required to produce Points from Columnar DataStream, " +
                        "but those wasn't specified");
            }

            final GeometryFactory geometryFactory = new GeometryFactory();
            final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();

            return new DataStream(StreamType.Point, ((JavaRDD<Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<PointEx> result = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar line = it.next();

                            double lat = line.asDouble(latColumn);
                            double lon = line.asDouble(lonColumn);

                            double radius = (radiusColumn != null) ? line.asDouble(radiusColumn) : defaultRadius;

                            Map<String, Object> props = new HashMap<>();
                            for (String col : _outputColumns) {
                                props.put(col, line.asIs(col));
                            }

                            PointEx point = new PointEx(csFactory.create(new Coordinate[]{new Coordinate(lon, lat, radius)}), geometryFactory);
                            point.setUserData(props);

                            result.add(point);
                        }

                        return result.iterator();
                    }), Collections.singletonMap(OBJLVL_POINT, _outputColumns));
        };
    }
}
