/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class PolygonEyeView extends Transform {
    public static final String AZIMUTH_PROP = "azimuth_prop";
    public static final String ANGLE_PROP = "angle_prop";
    public static final String DEFAULT_ANGLE = "angle_default";
    static final String GEN_AZIMUTH = "_azimuth";
    static final String GEN_ANGLE = "_angle";
    static final String GEN_RADIUS = "_radius";

    @Override
    public PluggableMeta initMeta() {
        return new PluggableMetaBuilder("polygonEyeView",
                "Create 'eye view' Polygons for POIs with set azimuth and view angle." +
                        " Names of referenced properties have to be same in each INPUT DataStream")
                .transform(StreamType.Point, StreamType.Polygon).objLvls(POLYGON).keyAfter().operation()
                .def(AZIMUTH_PROP, "Azimuth property of POIs, degrees. Counts clockwise from north, +90 is due east, -90 is due west")
                .def(ANGLE_PROP, "Viewing angle property of POIs, degrees", null, "By default, viewing angle property isn't set")
                .def(DEFAULT_ANGLE, "Default viewing angle of POIs, degrees", Double.class,
                        110.D, "By default, viewing angle of POIs is 110 degrees")
                .generated(GEN_AZIMUTH, "Azimuth property")
                .generated(GEN_ANGLE, "Viewing angle property")
                .generated(GEN_RADIUS, "Radius property")
                .build();
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = (newColumns != null) ? newColumns.get(POLYGON) : null;
            if (valueColumns == null) {
                valueColumns = ds.attributes(VALUE);
            }
            final List<String> _outputColumns = valueColumns;

            final String _azimuthColumn = params.get(AZIMUTH_PROP);
            final String _angleColumn = params.get(ANGLE_PROP);
            final double _defaultAngle = params.get(DEFAULT_ANGLE);

            final GeometryFactory geometryFactory = new GeometryFactory();

            JavaPairRDD<Object, DataRecord<?>> out = ds.rdd()
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        double radius, azimuth, angle = _defaultAngle;
                        double lat, lon, angleInc;
                        Coordinate[] coords;
                        GeodesicData gd;
                        Tuple2<Object, DataRecord<?>> poi;
                        SpatialRecord<?> p;
                        PointEx c;
                        while (it.hasNext()) {
                            poi = it.next();

                            p = (SpatialRecord<?>) poi._2;
                            c = (PointEx) p.getCentroid();
                            radius = c.getRadius();
                            azimuth = p.asDouble(_azimuthColumn);
                            if (_angleColumn != null) {
                                angle = p.asDouble(_angleColumn);
                            }

                            coords = new Coordinate[15];

                            lat = c.getY();
                            lon = c.getX();
                            coords[14] = coords[0] = new Coordinate(lon, lat);
                            angleInc = angle / 12;

                            for (int j = -6; j <= 6; j++) {
                                gd = Geodesic.WGS84.Direct(lat, lon, azimuth + angleInc * j, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                coords[j + 7] = new Coordinate(gd.lon2, gd.lat2);
                            }

                            PolygonEx poly = new PolygonEx(geometryFactory.createPolygon(coords));
                            for (String column : _outputColumns) {
                                switch (column) {
                                    case GEN_AZIMUTH: {
                                        poly.put(GEN_AZIMUTH, azimuth);
                                        break;
                                    }
                                    case GEN_ANGLE: {
                                        poly.put(GEN_ANGLE, angle);
                                        break;
                                    }
                                    case GEN_RADIUS: {
                                        poly.put(GEN_RADIUS, radius);
                                        break;
                                    }
                                    default: {
                                        poly.put(column, p.asIs(column));
                                    }
                                }
                            }

                            ret.add(new Tuple2<>(poi._1, poly));
                        }

                        return ret.iterator();
                    });

            return new DataStreamBuilder(ds.name, Collections.singletonMap(POLYGON, _outputColumns))
                    .generated(meta.verb, StreamType.Polygon, ds)
                    .build(out);
        };
    }
}
