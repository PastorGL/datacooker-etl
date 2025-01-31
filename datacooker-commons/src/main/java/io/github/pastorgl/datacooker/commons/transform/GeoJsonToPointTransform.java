/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.scripting.Utils;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.POINT;

@SuppressWarnings("unused")
public class GeoJsonToPointTransform extends Transform {
    static final String RADIUS_DEFAULT = "radius_default";
    static final String RADIUS_PROP = "radius_prop";

    @Override
    public TransformMeta meta() {
        return new TransformMeta("geoJsonToPoint", StreamType.PlainText, StreamType.Point,
                "Take Plain Text representation of GeoJSON fragment file and produce a Point DataStream." +
                        " Does not preserve partitioning",

                new DefinitionMetaBuilder()
                        .def(RADIUS_DEFAULT, "If set, generated Points will have this value in the radius attribute",
                                Double.class, Double.NaN, "By default, don't add radius attribute to Points")
                        .def(RADIUS_PROP, "If set, generated Points will use this JSON property as radius",
                                Double.class, null, "By default, don't add radius attribute to Points")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            String radiusColumn = params.get(RADIUS_PROP);
            final double defaultRadius = params.get(RADIUS_DEFAULT);

            List<String> _outputColumns = newColumns.get(POINT);

            return new DataStreamBuilder(ds.name, newColumns)
                    .transformed(meta.verb, StreamType.Point, ds)
                    .build(ds.rdd().flatMapToPair(line -> {
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
                                Map<String, Object> properties = feature.getProperties();

                                List<PointEx> points = new ArrayList<>();
                                if (geometry instanceof Polygon) {
                                    points.add(new PointEx(geometry.getCentroid()));
                                } else if (geometry instanceof MultiPolygon) {
                                    for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                        points.add(new PointEx(geometry.getGeometryN(i).getCentroid()));
                                    }
                                } else if (geometry instanceof Point) {
                                    points.add(new PointEx(geometry));
                                } else if (geometry instanceof MultiPoint) {
                                    for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                        points.add(new PointEx(geometry.getGeometryN(i)));
                                    }
                                }

                                for (PointEx point : points) {
                                    Map<String, Object> props = new HashMap<>();
                                    if (_outputColumns != null) {
                                        for (String col : _outputColumns) {
                                            props.put(col, properties.get(col));
                                        }
                                    } else {
                                        props.putAll(properties);
                                    }
                                    point.put(props);

                                    double radius;
                                    if (radiusColumn != null) {
                                        radius = Utils.parseNumber(String.valueOf(properties.get(radiusColumn))).doubleValue();
                                    } else {
                                        radius = defaultRadius;
                                    }
                                    point.setRadius(radius);

                                    ret.add(new Tuple2<>(line._1, point));
                                }
                            }
                        }

                        return ret.iterator();
                    }));
        };
    }
}
