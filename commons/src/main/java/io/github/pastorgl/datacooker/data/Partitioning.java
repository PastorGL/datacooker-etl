/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.metadata.DefinitionEnum;

public enum Partitioning implements DefinitionEnum {
    HASHCODE("Record hashcode"),
    RANDOM("A random number"),
    SOURCE("Source location");

    private final String descr;

    Partitioning(String descr) {
        this.descr = descr;
    }

    @Override
    public String descr() {
        return descr;
    }
}
