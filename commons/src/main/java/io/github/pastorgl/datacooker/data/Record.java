/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import java.util.Map;

public interface Record<T> {
    T put(Map<String, Object> payload);

    T put(String attr, Object value);

    byte[] asBytes(String attr);

    Double asDouble(String attr);

    Integer asInt(String attr);

    Object asIs(String attr);

    Long asLong(String attr);

    String asString(String attr);

    Object clone();
}
