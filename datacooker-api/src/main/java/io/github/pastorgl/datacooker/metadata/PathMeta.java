/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

public class PathMeta implements InputOutputMeta {
    public final String[] examples;
    public final boolean wildcard;

    public PathMeta(String[] examples, boolean wildcard) {
        this.examples = examples;
        this.wildcard = wildcard;
    }
}
