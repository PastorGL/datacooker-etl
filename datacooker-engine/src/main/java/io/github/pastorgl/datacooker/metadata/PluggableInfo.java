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
    private final Pluggable<?, ?> pInstance;

    public PluggableInfo(PluggableMeta meta, Class<Pluggable<?, ?>> pClass) {
        this.meta = meta;

        this.pClass = pClass;
        this.pInstance = null;
    }

    public PluggableInfo(PluggableMeta meta, Pluggable<?, ?> pInstance) {
        this.meta = meta;

        this.pClass = null;
        this.pInstance = pInstance;
    }

    public Pluggable<?, ?> instance() throws Exception {
        if (pClass == null) {
            return pInstance;
        }

        return pClass.getDeclaredConstructor().newInstance();
    }

    @JsonCreator
    public PluggableInfo(PluggableMeta meta) {
        this.meta = meta;

        this.pClass = null;
        this.pInstance = null;
    }
}
