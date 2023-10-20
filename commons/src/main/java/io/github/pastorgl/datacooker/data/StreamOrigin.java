/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.metadata.DefinitionEnum;

public enum StreamOrigin implements DefinitionEnum {
    CREATED("Newly created"),
    FILTERED("Input records unchanged, just filtered"),
    AUGMENTED("Input records augmented with generated or other inputs' attributes, may be filtered too"),
    GENERATED("Records somehow generated from input(s) but with no guaranteed 1-to-1 relation"),
    TRANSFORMED("Input records transformed, may be augmented too"),
    ALTERED("Stream changed, but not its records"),
    PASSEDTHRU("Stream left unchanged");

    private final String descr;

    StreamOrigin(String descr) {
        this.descr = descr;
    }

    @Override
    public String descr() {
        return descr;
    }
}
