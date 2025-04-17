/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.Serializable;

public class PluggableInfo implements Serializable {
    public final PluggableMeta meta;
    private final Class<Pluggable<?, ?>> pClass;

    public PluggableInfo(PluggableMeta meta, Class<Pluggable<?, ?>> pClass) {
        this.pClass = pClass;
        this.meta = meta;
    }

    public Pluggable<?, ?> newInstance() throws Exception {
        return pClass.getDeclaredConstructor().newInstance();
    }

    @JsonCreator
    public PluggableInfo(PluggableMeta meta) {
        this.meta = meta;
        this.pClass = null;
    }
}
