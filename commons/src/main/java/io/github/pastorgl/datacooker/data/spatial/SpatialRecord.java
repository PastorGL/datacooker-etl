/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import io.github.pastorgl.datacooker.data.Record;
import org.locationtech.jts.geom.Geometry;

import java.util.Map;

public interface SpatialRecord<T extends Geometry> extends Record<T> {
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
            p = (p == null) ? null : Integer.parseInt(String.valueOf(p));
        }

        return (Integer) p;
    }

    @Override
    default Double asDouble(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        if (!(p instanceof Double)) {
            p = (p == null) ? null : Double.parseDouble(String.valueOf(p));
        }

        return (Double) p;
    }

    @Override
    default Long asLong(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);
        if (!(p instanceof Long)) {
            p = (p == null) ? null : Long.parseLong(String.valueOf(p));
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
    default Object asIs(String property) {
        Object p = ((Map<String, Object>) ((T) this).getUserData()).get(property);

        return p;
    }
}
