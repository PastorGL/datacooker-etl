/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

public enum Origin {
    //outputs source records unchanged, just filtered
    FILTERED,
    //outputs source records augmented with generated props, may filter them too
    AUGMENTED,
    //outputs records somehow generated from input but with no 1:1 relation
    GENERATED
}
