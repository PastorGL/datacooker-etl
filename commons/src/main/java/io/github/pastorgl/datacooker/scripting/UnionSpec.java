/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.metadata.DefinitionEnum;

public enum UnionSpec implements DefinitionEnum {
    CONCAT("Just concatenate inputs, don't look into records"),
    XOR("Only emit records that occur strictly in one input RDD"),
    AND("Only emit records that occur in all input RDDs");

    private final String descr;

    UnionSpec(String descr) {
        this.descr = descr;
    }

    @Override
    public String descr() {
        return descr;
    }
}
