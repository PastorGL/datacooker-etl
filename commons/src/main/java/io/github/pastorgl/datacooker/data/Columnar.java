/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.List;
import java.util.Map;

public class Columnar implements KryoSerializable, Record<Columnar> {
    protected ListOrderedMap<String, Object> payload;

    public Columnar() {
        this.payload = new ListOrderedMap<>();
    }

    public Columnar(List<String> columns) {
        payload = new ListOrderedMap<>();
        columns.forEach(e -> payload.put(e, null));
    }

    public Columnar(List<String> columns, Object[] payload) {
        this.payload = new ListOrderedMap<>();
        for (int i = 0; i < columns.size(); i++) {
            this.payload.put(columns.get(i), payload[i]);
        }
    }

    @Override
    public List<String> attrs() {
        return payload.keyList();
    }

    public Columnar put(Map<String, Object> payload) {
        this.payload.putAll(payload);
        return this;
    }

    public Columnar put(String column, Object payload) {
        if (!(payload == null || payload instanceof Integer || payload instanceof Double || payload instanceof Long || payload instanceof byte[] || payload instanceof String)) {
            throw new RuntimeException("Attempt to put payload of wrong type into columnar record");
        }

        this.payload.put(column, payload);
        return this;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        try {
            byte[] arr = BSON.writeValueAsBytes(payload);
            output.writeInt(arr.length);
            output.write(arr, 0, arr.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        try {
            int length = input.readInt();
            byte[] bytes = input.readBytes(length);
            payload = BSON.readValue(bytes, ListOrderedMap.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Integer asInt(String attr) {
        Object p = payload.get(attr);
        if (!(p instanceof Integer)) {
            p = (p instanceof Boolean) ? null : Integer.parseInt(String.valueOf(p));
            payload.put(attr, p);
        }

        return (Integer) p;
    }

    public Double asDouble(String attr) {
        Object p = payload.get(attr);
        if (!(p instanceof Double)) {
            p = (p instanceof Boolean) ? null : Double.parseDouble(String.valueOf(p));
            payload.put(attr, p);
        }

        return (Double) p;
    }

    public Long asLong(String attr) {
        Object p = payload.get(attr);
        if (!(p instanceof Long)) {
            p = (p instanceof Boolean) ? null : Long.parseLong(String.valueOf(p));
            payload.put(attr, p);
        }

        return (Long) p;
    }

    public byte[] asBytes(String attr) {
        Object p = payload.get(attr);
        if ((p == null) || !p.getClass().isArray()) {
            p = (p instanceof Boolean) ? null : String.valueOf(p).getBytes();
            payload.put(attr, p);
        }

        return (byte[]) p;
    }

    public String asString(String attr) {
        Object p = payload.get(attr);
        String s = null;
        if (!(p instanceof String)) {
            if (p instanceof Integer) {
                s = String.valueOf(p);
            }
            if (p instanceof Double) {
                s = String.valueOf(p);
            }
            if (p instanceof byte[]) {
                s = new String((byte[]) p);
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
            payload.put(attr, p);
        }

        return p;
    }

    public int length() {
        return payload.size();
    }

    public ListOrderedMap<String, Object> asIs() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Columnar)) return false;
        return payload.equals(((Columnar) o).payload);
    }

    @Override
    public int hashCode() {
        return payload.hashCode();
    }
}
