/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.Utils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface SpatialRecord<T extends Geometry> extends DataRecord<T> {
    GeometryFactory FACTORY = new GeometryFactory();

    @Override
    default List<String> attrs() {
        return new ArrayList<>(((Map<String, Object>) ((T) this).getUserData()).keySet());
    }

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
}
