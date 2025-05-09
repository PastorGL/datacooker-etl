/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Utils;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;

@SuppressWarnings("unused")
public class GeoJsonToRoadMap extends Transformer {
    public static final String NAME_PROP = "name_prop";
    public static final String TYPE_PROP = "type_prop";
    public static final String WIDTH_PROP = "width_prop";
    public static final String ROAD_TYPES = "road_types";
    public static final String TYPE_MULTIPLIER_PREFIX = "type_multiplier_";
    static final String VERB = "geoJsonToRoadMap";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Generate a Polygon DataStream with road map coverage from the GeoJSON fragments exported from OSM." +
                        " Does not preserve partitioning")
                .transform().objLvls(POLYGON)
                .input(StreamType.PLAIN_TEXT, "Input OSM GeoJson DS")
                .output(StreamType.POLYGON, "Output Polygon DS")
                .def(NAME_PROP, "Feature property with road name")
                .def(TYPE_PROP, "Feature property with target road type")
                .def(WIDTH_PROP, "Feature property with road width (i.e. number of lanes)")
                .def(ROAD_TYPES, "Target road types", Object[].class,
                        new Object[]{"primary", "secondary", "tertiary"}, "Default target road types")
                .dynDef(TYPE_MULTIPLIER_PREFIX, "Multipliers to adjust road width for each target type (i.e. lane width in meters)",
                        Double.class)
                .generated("*", "All properties enumerated as columns from 'polygon' category")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            Object[] roadTypes = params.get(ROAD_TYPES);

            final HashMap<String, Double> multipliers = new HashMap<>();
            for (Object roadType : roadTypes) {
                Double multiplier = params.get(TYPE_MULTIPLIER_PREFIX + roadType);
                multipliers.put(roadType.toString(), multiplier);
            }

            final String typeColumn = params.get(TYPE_PROP);
            final String widthColumn = params.get(WIDTH_PROP);
            final String nameColumn = params.get(NAME_PROP);

            final List<String> _outputColumns = newColumns.get(POLYGON);

            final GeometryFactory geometryFactory = new GeometryFactory();

            JavaPairRDD<Object, DataRecord<?>> polygons = ds.rdd()
                    .flatMapToPair(line -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        GeoJSONReader reader = new GeoJSONReader();

                        GeoJSON json = GeoJSONFactory.create(String.valueOf(line._2));
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

                                                    double radius = Utils.parseNumber(String.valueOf(width)).doubleValue() * multipliers.get(roadType) / 2;

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
                                                        poly.put(properties);
                                                        ret.add(new Tuple2<>(line._1, poly));

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
                                                            poly.put(properties);
                                                            ret.add(new Tuple2<>(line._1, poly));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        return ret.iterator();
                    });

            Map<ObjLvl, List<String>> columns = (_outputColumns != null) ? newColumns : Collections.singletonMap(POLYGON, Arrays.asList(typeColumn, widthColumn, nameColumn));
            return new DataStreamBuilder(outputName, columns)
                    .transformed(VERB, StreamType.Polygon, ds)
                    .build(polygons);
        };
    }
}
