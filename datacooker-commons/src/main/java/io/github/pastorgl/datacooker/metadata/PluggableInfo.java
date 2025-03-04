/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

public class PluggableInfo {
    public final Class<?> pluggable;
    public final PluggableMeta meta;

    public PluggableInfo(Class<?> pluggable, PluggableMeta meta) {
        this.pluggable = pluggable;
        this.meta = meta;
    }
}
