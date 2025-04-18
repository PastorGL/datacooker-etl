/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.transform;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;

@SuppressWarnings("unused")
public class GeoJsonToPolygonTransform extends Transformer {

    static final String VERB = "geoJsonToPolygon";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Take Plain Text representation of GeoJSON fragment file and produce a Polygon DataStream." +
                        " Does not preserve partitioning")
                .transform().objLvls(POLYGON).operation()
                .input(StreamType.PLAIN_TEXT, "Input GeoJson DS")
                .output(StreamType.POLYGON, "Output Polygon DS")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return (ds, newColumns, params) -> {
            List<String> _outputColumns = (newColumns != null) ? newColumns.get(POLYGON) : null;

            return new DataStreamBuilder(outputName, newColumns)
                    .transformed(VERB, StreamType.Polygon, ds)
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

                                List<Geometry> geometries = new ArrayList<>();
                                if (geometry instanceof Polygon) {
                                    geometries.add(geometry);
                                } else if (geometry instanceof MultiPolygon) {
                                    for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                        geometries.add(geometry.getGeometryN(i));
                                    }
                                }

                                for (Geometry gg : geometries) {
                                    Map<String, Object> props = new HashMap<>();
                                    if (_outputColumns != null) {
                                        for (String col : _outputColumns) {
                                            props.put(col, properties.get(col));
                                        }
                                    } else {
                                        props.putAll(properties);
                                    }

                                    PolygonEx polygon = new PolygonEx(gg);
                                    polygon.put(props);

                                    ret.add(new Tuple2<>(line._1, polygon));
                                }
                            }
                        }

                        return ret.iterator();
                    }));
        };
    }
}
