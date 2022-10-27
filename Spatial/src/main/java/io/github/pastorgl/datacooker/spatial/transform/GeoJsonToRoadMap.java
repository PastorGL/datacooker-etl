/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.metadata.TransformedStreamMetaBuilder;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_POLYGON;

@SuppressWarnings("unused")
public class GeoJsonToRoadMap implements Transform {
    public static final String NAME_PROP = "name_prop";
    public static final String TYPE_PROP = "type_prop";
    public static final String WIDTH_PROP = "width_prop";
    public static final String ROAD_TYPES = "road_types";
    public static final String TYPE_MULTIPLIER_PREFIX = "type_multiplier_";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("geoJsonToRoadMap", StreamType.PlainText, StreamType.Polygon,
                "Generate a Polygon DataStream with road map coverage from the GeoJSON fragments exported from OSM",

                new DefinitionMetaBuilder()
                        .def(NAME_PROP, "Feature property with road name")
                        .def(TYPE_PROP, "Feature property with target road type")
                        .def(WIDTH_PROP, "Feature property with road width (i.e. number of lanes)")
                        .def(ROAD_TYPES, "Target road types", String[].class,
                                new String[]{"primary", "secondary", "tertiary"}, "Default target road types")
                        .dynDef(TYPE_MULTIPLIER_PREFIX, "Multipliers to adjust road width for each target type (i.e. lane width in meters)",
                                Double.class)
                        .build(),

                new TransformedStreamMetaBuilder()
                        .genCol("*", "All properties enumerated as columns from 'polygon' category")
                        .build()
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            String[] roadTypes = params.get(ROAD_TYPES);

            final HashMap<String, Double> multipliers = new HashMap<>();
            for (String roadType : roadTypes) {
                Double multiplier = params.get(TYPE_MULTIPLIER_PREFIX + roadType);
                multipliers.put(roadType, multiplier);
            }

            final String typeColumn = params.get(TYPE_PROP);
            final String widthColumn = params.get(WIDTH_PROP);
            final String nameColumn = params.get(NAME_PROP);

            final List<String> _outputColumns = newColumns.get(OBJLVL_POLYGON);

            final GeometryFactory geometryFactory = new GeometryFactory();

            JavaRDD<PolygonEx> polygons = ((JavaRDD<Object>) ds.get())
                    .flatMap(line -> {
                        List<PolygonEx> result = new ArrayList<>();

                        GeoJSONReader reader = new GeoJSONReader();

                        GeoJSON json = GeoJSONFactory.create(String.valueOf(line));
                        List<Feature> features = null;
                        if (json instanceof Feature) {
                            features = Collections.singletonList((Feature) json);
                        } else if (json instanceof FeatureCollection) {
                            features = Arrays.asList(((FeatureCollection) json).getFeatures());
                        }

                        if (features != null) {
                            for (Feature feature : features) {
                                Geometry geometry = reader.read(feature.getGeometry());

                                Map<String, Object> featureProps = feature.getProperties();
                                String roadType = String.valueOf(featureProps.get(typeColumn));
                                Optional<Object> roadName = Optional.ofNullable(featureProps.get(nameColumn));

                                if (multipliers.containsKey(roadType) && roadName.isPresent()) {
                                    Object width = featureProps.get(widthColumn);
                                    if (width != null) {
                                        List<Geometry> geometries = new ArrayList<>();

                                        if (geometry instanceof LineString) {
                                            geometries.add(geometry);
                                        } else if (geometry instanceof MultiLineString) {
                                            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                                geometries.add(geometry.getGeometryN(i));
                                            }
                                        }

                                        int numSeg = geometries.size();
                                        if (numSeg > 0) {
                                            Map<String, Object> properties = (_outputColumns == null) ? new HashMap<>(featureProps) : featureProps.entrySet().stream()
                                                    .filter(e -> _outputColumns.contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                                            for (Geometry g : geometries) {
                                                int pointNum = g.getNumPoints();
                                                if (pointNum > 1) {
                                                    LineString ls = (LineString) g;

                                                    Point[] trk = new Point[pointNum];
                                                    for (int i = 0; i < pointNum; i++) {
                                                        trk[i] = ls.getPointN(i);
                                                    }

                                                    double radius = Double.parseDouble(String.valueOf(width)) * multipliers.get(roadType) / 2;

                                                    GeodesicData gd;
                                                    Coordinate[] c;
                                                    Point prevPoint, point;
                                                    double prevY, prevX, pointY, pointX, azi2;
                                                    for (int i = 0; i < trk.length; i++) {
                                                        point = trk[i];
                                                        pointY = point.getY();
                                                        pointX = point.getX();

                                                        c = new Coordinate[13];
                                                        for (int a = -180, j = 0; a < 180; a += 30, j++) {
                                                            gd = Geodesic.WGS84.Direct(pointY, pointX, a, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                            c[j] = new CoordinateXY(gd.lon2, gd.lat2);
                                                        }
                                                        c[12] = c[0];
                                                        PolygonEx poly = new PolygonEx(geometryFactory.createPolygon(c));
                                                        poly.setUserData(properties);
                                                        result.add(poly);

                                                        if (i != 0) {
                                                            prevPoint = trk[i - 1];
                                                            prevY = prevPoint.getY();
                                                            prevX = prevPoint.getX();
                                                            gd = Geodesic.WGS84.Inverse(prevY, prevX, pointY, pointX, GeodesicMask.AZIMUTH);
                                                            azi2 = gd.azi2;

                                                            c = new Coordinate[5];
                                                            gd = Geodesic.WGS84.Direct(pointY, pointX, azi2 + 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                            c[0] = new CoordinateXY(gd.lon2, gd.lat2);
                                                            gd = Geodesic.WGS84.Direct(pointY, pointX, azi2 - 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                            c[1] = new CoordinateXY(gd.lon2, gd.lat2);
                                                            gd = Geodesic.WGS84.Direct(prevY, prevX, azi2 - 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                            c[2] = new CoordinateXY(gd.lon2, gd.lat2);
                                                            gd = Geodesic.WGS84.Direct(prevY, prevX, azi2 + 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                            c[3] = new CoordinateXY(gd.lon2, gd.lat2);
                                                            c[4] = c[0];

                                                            if (Orientation.isCCW(c)) {
                                                                ArrayUtils.reverse(c);
                                                            }

                                                            poly = new PolygonEx(geometryFactory.createPolygon(c));
                                                            poly.setUserData(properties);
                                                            result.add(poly);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        return result.iterator();
                    });

            Map<String, List<String>> columns = (_outputColumns != null) ? newColumns : Collections.singletonMap(OBJLVL_POLYGON, Arrays.asList(typeColumn, widthColumn, nameColumn));
            return new DataStream(StreamType.Polygon, polygons, columns);
        };
    }
}
