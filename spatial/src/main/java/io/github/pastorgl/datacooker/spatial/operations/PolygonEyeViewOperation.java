/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.metadata.StreamOrigin;
import io.github.pastorgl.datacooker.scripting.Operation;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POINT;
import static io.github.pastorgl.datacooker.Constants.OBJLVL_POLYGON;

@SuppressWarnings("unused")
public class PolygonEyeViewOperation extends Operation {
    public static final String AZIMUTH_PROP = "azimuth_prop";
    public static final String ANGLE_PROP = "angle_prop";
    public static final String DEFAULT_ANGLE = "angle_default";
    static final String GEN_AZIMUTH = "_azimuth";
    static final String GEN_ANGLE = "_angle";
    static final String GEN_RADIUS = "_radius";

    private String azimuth;
    private String angle;

    private Double defaultAngle;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonEyeView", "Create 'eye view' Polygons for POIs with set azimuth and view angle." +
                " Names of referenced properties have to be same in each INPUT DataStream",

                new PositionalStreamsMetaBuilder()
                        .input("Source Spatial objects DataStream with Points Of Interest",
                                StreamType.SPATIAL
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(AZIMUTH_PROP, "Azimuth property of POIs, degrees. Counts clockwise from north, +90 is due east, -90 is due west")
                        .def(ANGLE_PROP, "Viewing angle property of POIs, degrees", null, "By default, viewing angle property isn't set")
                        .def(DEFAULT_ANGLE, "Default viewing angle of POIs, degrees", Double.class,
                                110.D, "By default, viewing angle of POIs is 110 degrees")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Output with eye view polygons",
                                new StreamType[]{StreamType.Polygon}, StreamOrigin.GENERATED, null
                        )
                        .generated(GEN_AZIMUTH, "Azimuth property")
                        .generated(GEN_ANGLE, "Viewing angle property")
                        .generated(GEN_RADIUS, "Radius property")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        azimuth = params.get(AZIMUTH_PROP);
        angle = params.get(ANGLE_PROP);
        defaultAngle = params.get(DEFAULT_ANGLE);
    }

    @Override
    public Map<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final String _azimuthColumn = azimuth;
        final String _angleColumn = (angle != null) ? angle : null;
        final double _defaultAngle = defaultAngle;

        Map<String, DataStream> output = new HashMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            final GeometryFactory geometryFactory = new GeometryFactory();

            JavaPairRDD<Object, Record<?>> out = input.rdd
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        double radius, azimuth, angle = _defaultAngle;
                        double lat, lon, angleInc;
                        Coordinate[] coords;
                        GeodesicData gd;
                        Tuple2<Object, Record<?>> poi;
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
                            poly.put(p.asIs());
                            poly.put(GEN_AZIMUTH, azimuth);
                            poly.put(GEN_ANGLE, angle);
                            poly.put(GEN_RADIUS, radius);

                            ret.add(new Tuple2<>(poi._1, poly));
                        }

                        return ret.iterator();
                    });

            List<String> outputColumns = new ArrayList<>(input.accessor.attributes(OBJLVL_POINT));
            outputColumns.add(GEN_ANGLE);
            outputColumns.add(GEN_AZIMUTH);
            outputColumns.add(GEN_RADIUS);

            output.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), StreamType.Polygon, Collections.singletonMap(OBJLVL_POLYGON, outputColumns))
                    .generated(meta.verb, input)
                    .build(out)
            );
        }

        return output;
    }
}
