/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.POINT;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class ColumnarToPointTransform extends Transform {
    static final String RADIUS_DEFAULT = "radius_default";
    static final String RADIUS_COLUMN = "radius_column";
    static final String LAT_COLUMN = "lat_column";
    static final String LON_COLUMN = "lon_column";
    static final String VERB = "columnarToPoint";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Transform Columnar DataStream to Point by setting coordinates and optional radius")
                .transform().objLvls(POINT).operation()
                .input(StreamType.COLUMNAR, "Input Columnar DS")
                .output(StreamType.POINT, "Output Point DS")
                .def(RADIUS_DEFAULT, "If set, generated Points will have this value in the radius parameter",
                        Double.class, Double.NaN, "By default, Point radius attribute is not set")
                .def(RADIUS_COLUMN, "If set, generated Points will take their radius parameter from the specified column instead",
                        null, "By default, don't set Point radius attribute")
                .def(LAT_COLUMN, "Point latitude column")
                .def(LON_COLUMN, "Point longitude column")
                .build();
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = (newColumns != null) ? newColumns.get(POINT) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(VALUE);
            }

            final List<String> _outputColumns = valueColumns;

            final String latColumn = params.get(LAT_COLUMN);
            final String lonColumn = params.get(LON_COLUMN);
            final String radiusColumn = params.get(RADIUS_COLUMN);
            final Double defaultRadius = params.get(RADIUS_DEFAULT);

            if ((latColumn == null) || (lonColumn == null)) {
                throw new InvalidConfigurationException("Parameters '" + LAT_COLUMN + "' and '" + LON_COLUMN
                        + "' are both required to produce Points from Columnar DataStream, but those wasn't specified");
            }

            final CoordinateSequenceFactory csFactory = SpatialRecord.FACTORY.getCoordinateSequenceFactory();

            return new DataStreamBuilder(ds.name, Collections.singletonMap(POINT, _outputColumns))
                    .transformed(VERB, StreamType.Point, ds)
                    .build(ds.rdd().mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> line = it.next();

                            double lat = line._2.asDouble(latColumn);
                            double lon = line._2.asDouble(lonColumn);

                            double radius = (radiusColumn != null) ? line._2.asDouble(radiusColumn) : defaultRadius;

                            Map<String, Object> props = new HashMap<>();
                            for (String col : _outputColumns) {
                                props.put(col, line._2.asIs(col));
                            }

                            PointEx point = new PointEx(csFactory.create(new Coordinate[]{new Coordinate(lon, lat, radius)}));
                            point.put(props);

                            ret.add(new Tuple2<>(line._1, point));
                        }

                        return ret.iterator();
                    }, true));
        };
    }
}
