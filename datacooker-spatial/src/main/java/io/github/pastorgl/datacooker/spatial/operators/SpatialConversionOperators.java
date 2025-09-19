/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.operators;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.Structured;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.scripting.Operator;
import io.github.pastorgl.datacooker.spatial.functions.PolygonConverter;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.Deque;
import java.util.Map;

@SuppressWarnings("unused")
public class SpatialConversionOperators {
    public static class POLYGON extends Operator.Unary<PolygonEx, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected PolygonEx op0(Deque<Object> args) {
            try {
                Object obj = args.pop();

                if (obj instanceof PolygonEx) {
                    return (PolygonEx) obj;
                } else {
                    GeoJSONReader reader = new GeoJSONReader();

                    Feature feature = (Feature) GeoJSONFactory.create(String.valueOf(obj));
                    PolygonEx polygon = new PolygonEx(reader.read(feature.getGeometry()));
                    polygon.put(feature.getProperties());

                    return polygon;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "POLYGON";
        }

        @Override
        public String descr() {
            return "Convert GeoJSON String to Polygon Object";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class POINT extends Operator.Unary<PointEx, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected PointEx op0(Deque<Object> args) {
            try {
                Object obj = args.pop();

                if (obj instanceof PointEx) {
                    return (PointEx) obj;
                } else {
                    GeoJSONReader reader = new GeoJSONReader();

                    Feature feature = (Feature) GeoJSONFactory.create(String.valueOf(obj));
                    PointEx point = new PointEx(reader.read(feature.getGeometry()));
                    point.put(feature.getProperties());

                    return point;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "POINT";
        }

        @Override
        public String descr() {
            return "Convert GeoJSON String to Point Object";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class GeoStructured extends Operator.Unary<Structured, SpatialRecord<?>> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected Structured op0(Deque<Object> args) {
            try {
                Object obj = args.pop();

                ObjectMapper om = new ObjectMapper();
                om.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
                String json;
                if (obj instanceof PointEx point) {
                    json = new Feature(new org.wololo.geojson.Point(new double[]{point.getX(), point.getY()}), (Map<String, Object>) point.getUserData()).toString();
                } else if (obj instanceof PolygonEx polygon) {
                    json = new Feature(PolygonConverter.convert(polygon), (Map<String, Object>) polygon.getUserData()).toString();
                } else {
                    return null;
                }

                return new Structured(om.readValue(json, Object.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "GEO_STRUCT";
        }

        @Override
        public String descr() {
            return "Convert a Polygon or Point to Structured Object";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }
}
