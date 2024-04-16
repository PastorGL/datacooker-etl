/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.scripting.Utils;

import java.util.*;

public class Structured implements KryoSerializable, Record<Structured> {
    protected Map<String, Object> payload;

    public Structured() {
        payload = new HashMap<>();
    }

    public Structured(Object o) {
        this();
        if (o instanceof Map) {
            put((Map<String, Object>) o);
        } else {
            payload.put("", o);
        }
    }

    public Structured(List<String> columns) {
        payload = new HashMap<>();
        columns.forEach(e -> payload.put(e, null));
    }

    public Structured(List<String> columns, Object[] payload) {
        this.payload = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            this.payload.put(columns.get(i), payload[i]);
        }
    }

    @Override
    public List<String> attrs() {
        return new ArrayList<>(payload.keySet());
    }

    public Structured put(Map<String, Object> payload) {
        this.payload.putAll(payload);
        return this;
    }

    public Structured put(String column, Object payload) {
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
            Object v = BSON.readValue(bytes, Object.class);
            payload = (v instanceof Map) ? new HashMap<>((Map) v) : Collections.singletonMap("", v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Object getArr(String attr, Object payload) {
        String[] deep = attr.split("[\\[\\]]", 3);

        Object[] arr;
        if (payload instanceof Map) {
            Object o = ((Map) payload).get(deep[0]);
            arr = (o instanceof Object[]) ? (Object[]) o : new Object[]{o};
        } else if (payload instanceof Object[]) {
            arr = (Object[]) payload;
        } else {
            arr = new Object[]{payload};
        }

        if (arr == null) {
            return null;
        }

        if (deep[2].isEmpty()) {
            if (Constants.STAR.equals(deep[1]) || deep[1].isEmpty()) {
                return arr;
            }
        }

        if (!deep[1].contains(":")) {
            return get(deep[2], arr[Utils.parseNumber(deep[1]).intValue()]);
        }

        if (!deep[2].isEmpty()) {
            throw new RuntimeException("Array slice must be in the final position, but got '" + attr + "' instead");
        }

        String[] range = deep[1].split(":", 2);
        Integer left = range[0].isEmpty() ? null : Utils.parseNumber(range[0]).intValue();
        Integer right = range[1].isEmpty() ? null : Utils.parseNumber(range[1]).intValue();

        if ((left == null) && (right == null)) {
            return arr;
        }

        if (left == null) {
            left = 0;
        }
        if (right == null) {
            right = arr.length;
        }
        if (left < 0) {
            left = arr.length + left;
            if (left < 0) {
                left = 0;
            }
        }
        if (right < 0) {
            right = arr.length + right;
            if (right < 0) {
                right = 0;
            }
        }
        if ((left <= right) && (left < arr.length)) {
            return Arrays.copyOfRange(arr, left, right);
        }

        return new Object[0];
    }

    protected Object getDot(String attr, Object payload) {
        String[] deep = attr.split("\\.", 2);

        if (payload instanceof Map) {
            return get(deep[1], ((Map) payload).get(deep[0]));
        }

        return null;
    }

    protected Object get(String attr, Object payload) {
        if ((attr == null) || (payload == null)) {
            return payload;
        }

        while (attr.startsWith(".")) {
            attr = attr.substring(1);
        }

        int dotIndex = attr.indexOf('.');
        int arrIndex = attr.indexOf('[');
        if ((dotIndex < 0) && (arrIndex < 0)) {
            return (payload instanceof Map) ? ((Map<String, Object>) payload).get(attr) : payload;
        }

        if (arrIndex < 0) {
            return getDot(attr, payload);
        }

        if (dotIndex < 0) {
            return getArr(attr, payload);
        }

        if (arrIndex < dotIndex) {
            return getArr(attr, payload);
        }

        if (dotIndex < arrIndex) {
            return getDot(attr, payload);
        }

        return ((Map<String, Object>) payload).get(attr);
    }

    public Integer asInt(String attr) {
        Object p = get(attr, payload);
        if (!(p instanceof Integer)) {
            p = (p instanceof Boolean) ? null : Utils.parseNumber(String.valueOf(p)).intValue();
        }

        return (Integer) p;
    }

    public Double asDouble(String attr) {
        Object p = get(attr, payload);
        if (!(p instanceof Double)) {
            p = (p instanceof Boolean) ? null : Utils.parseNumber(String.valueOf(p)).doubleValue();
        }

        return (Double) p;
    }

    public Long asLong(String attr) {
        Object p = get(attr, payload);
        if (!(p instanceof Long)) {
            p = (p instanceof Boolean) ? null : Utils.parseNumber(String.valueOf(p)).longValue();
        }

        return (Long) p;
    }

    public byte[] asBytes(String attr) {
        Object p = get(attr, payload);
        if ((p == null) || !p.getClass().isArray()) {
            p = (p instanceof Boolean) ? null : String.valueOf(p).getBytes();
        }

        return (byte[]) p;
    }

    public String asString(String attr) {
        Object p = get(attr, payload);
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
    public Structured clone() {
        Structured rec = new Structured();
        rec.put(this.asIs());

        return rec;
    }

    public Object asIs(String attr) {
        if (attr == null) {
            return this;
        }
        return get(attr, payload);
    }

    public int length() {
        return payload.size();
    }

    public Map<String, Object> asIs() {
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

    @Override
    public String toString() {
        try {
            return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(payload);
        } catch (Exception e) {
            return payload.toString();
        }
    }
}
