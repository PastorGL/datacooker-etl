/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import java.util.Arrays;
import java.util.Collection;

public class ArrayWrap {
    public final Object[] data;

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
}
