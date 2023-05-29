/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PlainText extends Text implements Record<PlainText> {
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
        return Double.parseDouble(toString());
    }

    @Override
    public Integer asInt(String attr) {
        return Integer.parseInt(toString());
    }

    @Override
    public Object asIs(String attr) {
        return this;
    }

    @Override
    public Long asLong(String attr) {
        return Long.parseLong(toString());
    }

    @Override
    public String asString(String attr) {
        return toString();
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
