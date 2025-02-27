/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

public class TransformedStreamMetaBuilder {
    private final TransformedStreamMeta meta;

    public TransformedStreamMetaBuilder() {
        this.meta = new TransformedStreamMeta();
    }

    public TransformedStreamMetaBuilder generated(String colName, String colDescr) {
        meta.stream.generated.put(colName, colDescr);

        return this;
    }

    public TransformedStreamMeta build() {
        return meta;
    }
}
