/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.metadata.StreamOrigin;

import java.util.List;

public class StreamLineage {
    public String source;
    public final List<String> ancestors;
    public final StreamOrigin origin;

    StreamLineage(String source, StreamOrigin origin, List<String> ancestors) {
        this.source = source;
        this.origin = origin;
        this.ancestors = ancestors;
    }
}
