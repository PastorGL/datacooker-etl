/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

public class ArrayWrap implements Serializable, KryoSerializable {
    private static final ObjectMapper BSON = new ObjectMapper(new BsonFactory()).enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);

    private Object[] data;

    public ArrayWrap(Object data) {
        if (data instanceof ArrayWrap) {
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

    public Object[] data() {
        return data;
    }

    public int length() {
        return data.length;
    }

    public Object get(int i) {
        return data[i];
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
            byte[] arr = BSON.writeValueAsBytes(data);
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
            data = BSON.readValue(bytes, Object[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
