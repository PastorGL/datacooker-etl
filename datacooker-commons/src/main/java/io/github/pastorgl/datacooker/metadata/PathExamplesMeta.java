/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

public class PathExamplesMeta implements InputOutputMeta {
    public final String[] paths;

    public PathExamplesMeta(String[] paths) {
        this.paths = paths;
    }
}
