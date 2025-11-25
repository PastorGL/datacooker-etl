/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.Utils;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.locationtech.jts.geom.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface SpatialRecord<T extends Geometry> extends DataRecord<T> {
    GeometryFactory FACTORY = new GeometryFactory();

    @Override
    default List<String> attrs() {
        return new ArrayList<>(((Map<String, Object>) ((T) this).getUserData()).keySet());
    }

    @Override
    default T put(Map payload) {
        ((Map<String, Object>) ((T) this).getUserData()).putAll(payload);
        return (T) this;
    }

    @Override
    default T put(String property, Object value) {
        ((Map<String, Object>) ((T) this).getUserData()).put(property, value);
        return (T) this;
    }

    @Override
    default Object remove(String property) {
        return ((Map<String, Object>) ((T) this).getUserData()).remove(property);
    }

    @Override
    default Integer asInt(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        if (!(p instanceof Integer)) {
            p = (p == null) ? null : Utils.parseNumber(String.valueOf(p)).intValue();
        }

        return (Integer) p;
    }

    @Override
    default Double asDouble(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        if (!(p instanceof Double)) {
            p = (p == null) ? null : Utils.parseNumber(String.valueOf(p)).doubleValue();
        }

        return (Double) p;
    }

    @Override
    default Long asLong(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        if (!(p instanceof Long)) {
            p = (p == null) ? null : Utils.parseNumber(String.valueOf(p)).longValue();
        }

        return (Long) p;
    }

    @Override
    default byte[] asBytes(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        if ((p == null) || !p.getClass().isArray()) {
            p = (p == null) ? null : String.valueOf(p).getBytes();
        }

        return (byte[]) p;
    }

    @Override
    default String asString(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        if (p == null) {
            return null;
        }

        String s;
        if (!(p instanceof String)) {
            if (p instanceof byte[]) {
                s = new String((byte[]) p);
            } else {
                s = String.valueOf(p);
            }
        } else {
            s = (String) p;
        }
        return s;
    }

    @Override
    default ArrayWrap asArray(String property) {
        Object o = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        return (o == null) ? new ArrayWrap() : new ArrayWrap(o);
    }

    @Override
    default Object asIs(String property) {
        if (property == null) {
            return this;
        }

        return ((Map<String, Object>) ((T) this).getUserData()).get(property);
    }

    default Map<String, Object> asIs() {
        return (Map<String, Object>) ((T) this).getUserData();
    }

    Point getCentroid();

    static PointEx getCentroid(Point centroid, Coordinate[] coordinates) {
        if (coordinates.length == 0) {
            return null;
        }

        Coordinate minX = new CoordinateXY(Double.POSITIVE_INFINITY, Double.NaN),
                minY = new CoordinateXY(Double.NaN, Double.POSITIVE_INFINITY),
                maxX = new CoordinateXY(Double.NEGATIVE_INFINITY, Double.NaN),
                maxY = new CoordinateXY(Double.NaN, Double.NEGATIVE_INFINITY);
        for (Coordinate c : coordinates) {
            if (c.x < minX.x) {
                minX = (Coordinate) c.clone();
            }
            if (c.x > maxX.x) {
                maxX = (Coordinate) c.clone();
            }
            if (c.y < minY.y) {
                minY = (Coordinate) c.clone();
            }
            if (c.y > maxY.y) {
                maxY = (Coordinate) c.clone();
            }
        }
        double radius = 0.D;
        for (Coordinate c : Arrays.asList(minX, minY, maxX, maxY)) {
            double r = Geodesic.WGS84.Inverse(c.y, c.x,
                    centroid.getY(), centroid.getX(), GeodesicMask.DISTANCE
            ).s12;
            if (r > radius) {
                radius = r;
            }
        }
        PointEx centrePoint = new PointEx(centroid);
        centrePoint.setRadius(radius);
        return centrePoint;
    }
}
