/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.metadata.TransformMeta;

public class TransformInfo {
    public final Class<? extends Transform> transformClass;
    public final TransformMeta meta;

    public TransformInfo(Class<? extends Transform> transformClass, TransformMeta meta) {
        this.transformClass = transformClass;
        this.meta = meta;
    }
}
