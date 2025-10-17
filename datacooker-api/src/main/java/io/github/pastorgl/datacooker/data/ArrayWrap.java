/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ArrayWrap implements Serializable, KryoSerializable {
    private Object[] data;

    @JsonCreator
    public ArrayWrap(Object data) {
        if (data == null) {
            this.data = new Object[0];
        } else if (data instanceof ArrayWrap) {
            this.data = ((ArrayWrap) data).data;
        } else if (data instanceof Collection) {
            this.data = ((Collection<?>) data).toArray();
        } else if (data.getClass().isArray()) {
            this.data = (Object[]) data;
        } else {
            this.data = new Object[]{data};
        }
    }

    public ArrayWrap() {
        this.data = new Object[0];
    }

    @JsonGetter
    public Object[] data() {
        return data;
    }

    public int length() {
        return data.length;
    }

    public Object get(int i) {
        return data[i];
    }

    public Object put(int i, Object v) {
        Object e = data[i];
        data[i] = v;
        return e;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        Object[] oo;
        if (o instanceof ArrayWrap) {
            oo = ((ArrayWrap) o).data;
        } else if (o instanceof Object[]) {
            oo = (Object[]) o;
        } else if (o instanceof Collection) {
            oo = ((Collection<?>) o).toArray();
        } else {
            return false;
        }

        return Arrays.equals(data, oo);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return Arrays.toString(data);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        try {
            byte[] arr = ObjMapper.BSON.writeValueAsBytes(data);
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
            data = ObjMapper.BSON.readValue(bytes, Object[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ArrayWrap fromRange(Number a, Number b) {
        Object[] values;
        if ((a instanceof Integer) && (b instanceof Integer)) {
            int ia = a.intValue();
            int ib = b.intValue();

            if (ia > ib) {
                values = IntStream.rangeClosed(ib, ia).boxed().toArray();
                ArrayUtils.reverse(values);
            }
            values = IntStream.rangeClosed(ia, ib).boxed().toArray();
        } else {
            long la = a.longValue();
            long lb = b.longValue();

            if (la > lb) {
                values = LongStream.rangeClosed(lb, la).boxed().toArray();
                ArrayUtils.reverse(values);
            }
            values = LongStream.rangeClosed(la, lb).boxed().toArray();
        }

        return new ArrayWrap(values);
    }
}
