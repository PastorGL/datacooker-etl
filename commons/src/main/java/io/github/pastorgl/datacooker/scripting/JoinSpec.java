/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.metadata.DefinitionEnum;

public enum JoinSpec implements DefinitionEnum {
    INNER("Inner join"),
    LEFT("Left outer join"),
    RIGHT("Right outer join"),
    OUTER("Full outer join"),
    LEFT_ANTI("Left anti join"),
    RIGHT_ANTI("Right anti join");

    private final String descr;

    JoinSpec(String descr) {
        this.descr = descr;
    }

    @Override
    public String descr() {
        return descr;
    }
}
