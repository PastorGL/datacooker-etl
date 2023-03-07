/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.flatbuffers.ArrayReadWriteBuf;
import com.google.flatbuffers.FlexBuffers;
import com.google.flatbuffers.FlexBuffersBuilder;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Columnar implements KryoSerializable, Record<Columnar> {
    protected ListOrderedMap<String, Object> payload;
    protected FlexBuffers.Map source;
    protected byte[] bytes;

    public Columnar() {
        this.payload = new ListOrderedMap<>();
    }

    public Columnar(List<String> columns) {
        payload = new ListOrderedMap<>();
        columns.forEach(e -> payload.put(e, null));
        regen();
    }

    public Columnar(List<String> columns, Object[] payload) {
        this.payload = new ListOrderedMap<>();
        for (int i = 0; i < columns.size(); i++) {
            this.payload.put(columns.get(i), payload[i]);
        }
        regen();
    }

    @Override
    public List<String> attrs() {
        FlexBuffers.KeyVector keys = source.keys();
        int size = keys.size();
        String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = keys.get(i).toString();
        }
        return Arrays.asList(ret);
    }

    public Columnar put(Map<String, Object> payload) {
        this.payload.putAll(payload);
        return regen();
    }

    public Columnar put(String column, Object payload) {
        if (!(payload == null || payload instanceof Integer || payload instanceof Double || payload instanceof Long || payload instanceof byte[] || payload instanceof String)) {
            throw new RuntimeException("Attempt to put payload of wrong type into columnar record");
        }

        this.payload.put(column, payload);
        return regen();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        try {
            output.writeInt(bytes.length);
            output.write(bytes, 0, bytes.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int length = input.readInt();
        bytes = input.readBytes(length);
        source = FlexBuffers.getRoot(new ArrayReadWriteBuf(bytes, bytes.length)).asMap();
        payload = new ListOrderedMap<>();
    }

    public Integer asInt(String attr) {
        Object p = payload.get(attr);
        if (!(p instanceof Integer)) {
            FlexBuffers.Reference r = getRef(attr);
            p = r.isBoolean() ? null : r.asInt();
            payload.put(attr, p);
        }

        return (Integer) p;
    }

    public Double asDouble(String attr) {
        Object p = payload.get(attr);
        if (!(p instanceof Double)) {
            FlexBuffers.Reference r = getRef(attr);
            p = r.isBoolean() ? null : r.asFloat();
            payload.put(attr, p);
        }

        return (Double) p;
    }

    public Long asLong(String attr) {
        Object p = payload.get(attr);
        if (!(p instanceof Long)) {
            FlexBuffers.Reference r = getRef(attr);
            p = r.isBoolean() ? null : r.asLong();
            payload.put(attr, p);
        }

        return (Long) p;
    }

    public byte[] asBytes(String attr) {
        Object p = payload.get(attr);
        if ((p == null) || !p.getClass().isArray()) {
            FlexBuffers.Reference r = getRef(attr);
            p = r.isBoolean() ? null : r.asBlob().getBytes();
            payload.put(attr, p);
        }

        return (byte[]) p;
    }

    public String asString(String attr) {
        Object p = payload.get(attr);
        String s = null;
        if (!(p instanceof String)) {
            FlexBuffers.Reference r = getRef(attr);
            if (r.isString()) {
                s = r.asString();
            }
            if (r.isInt()) {
                s = String.valueOf(r.asLong());
            }
            if (r.isFloat()) {
                s = String.valueOf(r.asFloat());
            }
            if (r.isBlob()) {
                s = new String(r.asBlob().getBytes());
            }
            payload.put(attr, s);
        } else {
            s = (String) p;
        }

        return s;
    }

    @Override
    public Columnar clone() {
        Columnar rec = new Columnar();
        rec.put(this.asIs());

        return rec;
    }

    public Object asIs(String attr) {
        Object p = payload.get(attr);
        if (p == null) {
            p = fromRef(attr);
            payload.put(attr, p);
        }

        return p;
    }

    private Object fromRef(String attr) {
        FlexBuffers.Reference r = getRef(attr);
        Object p = null;
        if (r.isString()) {
            p = r.asString();
        }
        if (r.isInt()) {
            p = r.asLong();
        }
        if (r.isFloat()) {
            p = r.asFloat();
        }
        if (r.isBlob()) {
            p = r.asBlob().getBytes();
        }
        if (r.isBoolean()) {
            p = null;
        }
        return p;
    }

    private FlexBuffers.Reference getRef(String attr) {
        FlexBuffers.KeyVector keys = source.keys();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            String key = keys.get(i).toString();
            if (attr.equals(key)) {
                return source.get(i);
            }
        }
        return source.get(size);
    }

    public int length() {
        return payload.size();
    }

    public ListOrderedMap<String, Object> asIs() {
        ListOrderedMap<String, Object> ret = new ListOrderedMap<>();
        if (!payload.isEmpty()) {
            payload.keyList().forEach(k -> ret.put(k, asIs(k)));
        } else {
            FlexBuffers.KeyVector keys = source.keys();
            int size = keys.size();
            for (int i = 0; i < size; i++) {
                String key = keys.get(i).toString();
                ret.put(key, asIs(key));
            }
        }
        return ret;
    }

    public byte[] raw() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Columnar)) return false;
        Columnar columnar = (Columnar) o;
        if (bytes == null) {
            regen();
        }
        if (columnar.bytes == null) {
            columnar.regen();
        }
        return Arrays.equals(bytes, columnar.bytes);
    }

    @Override
    public int hashCode() {
        if (bytes == null) {
            regen();
        }
        return Arrays.hashCode(bytes);
    }

    protected Columnar regen() {
        FlexBuffersBuilder fbb = new FlexBuffersBuilder();
        int start = fbb.startMap();
        for (int index = 0, size = payload.size(); index < size; index++) {
            Object value = payload.getValue(index);
            String key = payload.get(index);
            if ((value == null) && (source != null)) {
                value = fromRef(key);
            }

            if (value == null) {
                fbb.putBoolean(key, false);
            } else if (value instanceof Integer) {
                fbb.putInt(key, (Integer) value);
            } else if (value instanceof Double) {
                fbb.putFloat(key, (Double) value);
            } else if (value instanceof Long) {
                fbb.putInt(key, (Long) value);
            } else if (value instanceof byte[]) {
                fbb.putBlob(key, (byte[]) value);
            } else {
                fbb.putString(key, String.valueOf(value));
            }
        }
        fbb.endMap(null, start);
        ByteBuffer buffer = fbb.finish();
        int length = buffer.remaining();
        bytes = new byte[length];
        System.arraycopy(buffer.array(), 0, bytes, 0, length);
        source = FlexBuffers.getRoot(new ArrayReadWriteBuf(bytes, length)).asMap();
        return this;
    }
}
