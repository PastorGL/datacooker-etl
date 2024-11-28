/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface DataRecord<T> extends Serializable {
    ObjectMapper BSON = new ObjectMapper(new BsonFactory()).enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);

    List<String> attrs();

    T put(Map<String, Object> payload);

    T put(String attr, Object value);

    byte[] asBytes(String attr);

    Double asDouble(String attr);

    Integer asInt(String attr);

    Object asIs(String attr);

    Long asLong(String attr);

    String asString(String attr);

    ArrayWrap asArray(String attr);

    Map<String, Object> asIs();

    Object clone();
}
