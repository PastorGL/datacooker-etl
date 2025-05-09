/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.scripting.Utils;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PlainText extends Text implements DataRecord<PlainText> {
    public PlainText(byte[] bytes) {
        super(bytes);
    }

    public PlainText(String string) {
        super(string);
    }

    @Override
    public List<String> attrs() {
        return Collections.emptyList();
    }

    @Override
    public PlainText put(Map<String, Object> payload) {
        if (payload.size() != 1) {
            throw new RuntimeException("PlainText payload must have exactly one attribute");
        }
        set(String.valueOf(payload.values().stream().findFirst()));
        return this;
    }

    @Override
    public PlainText put(String attr, Object value) {
        set(String.valueOf(value));
        return this;
    }

    @Override
    public byte[] asBytes(String attr) {
        return getBytes();
    }

    @Override
    public Double asDouble(String attr) {
        return Utils.parseNumber(toString()).doubleValue();
    }

    @Override
    public Integer asInt(String attr) {
        return Utils.parseNumber(toString()).intValue();
    }

    @Override
    public Object asIs(String attr) {
        return this;
    }

    @Override
    public Long asLong(String attr) {
        return Utils.parseNumber(toString()).longValue();
    }

    @Override
    public String asString(String attr) {
        return toString();
    }

    @Override
    public ArrayWrap asArray(String attr) {
        return new ArrayWrap(toString());
    }

    @Override
    public Map<String, Object> asIs() {
        return Collections.singletonMap("", this);
    }

    @Override
    public Object clone() {
        return new PlainText(getBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getBytes());
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
