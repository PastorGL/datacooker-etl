/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.metadata.DataStreamMeta;
import io.github.pastorgl.datacooker.metadata.DataStreamsMeta;
import io.github.pastorgl.datacooker.metadata.Origin;

public class TransformedStreamMeta extends DataStreamsMeta {
    public final DataStreamMeta streams;

    TransformedStreamMeta() {
        this.streams = new DataStreamMeta(null, null, false, Origin.GENERATED, null);
    }
}
