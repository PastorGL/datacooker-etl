/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

public class AnonymousInputBuilder {
    private final AnonymousStreamMeta meta;

    public AnonymousInputBuilder(String descr, StreamTypes type) {
        meta = new AnonymousStreamMeta(descr, type);
    }

    public AnonymousStreamMeta build() {
        return meta;
    }
}
