/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import java.util.List;
import java.util.Map;

public class DSData {
    public final Map<String, List<String>> attrs;
    public final String sl;
    public final String streamType;
    public final int numPartitions;
    public final int usages;

    public DSData(Map<String, List<String>> attrs, String sl, String streamType, int numPartitions, int usages) {
        this.attrs = attrs;
        this.sl = sl;
        this.streamType = streamType;
        this.numPartitions = numPartitions;
        this.usages = usages;
    }
}
