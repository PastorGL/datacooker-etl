/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import org.apache.commons.collections4.map.ListOrderedMap;

public abstract class ParamsBuilder<T extends ParamsBuilder<?>> {
    protected final ListOrderedMap<String, Param> params = new ListOrderedMap<>();

    public T mandatory(String name) {
        params.put(name, new Param());
        return (T) this;
    }

    public T optional(String name, Object value) {
        params.put(name, new Param(value));
        return (T) this;
    }
}
