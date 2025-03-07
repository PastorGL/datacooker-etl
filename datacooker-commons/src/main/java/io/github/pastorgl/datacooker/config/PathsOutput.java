/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.config;

import io.github.pastorgl.datacooker.data.ObjLvl;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

public class PathsOutput extends PathsInputOutput {
    public final Map<ObjLvl, List<String>> requested;

    public PathsOutput(JavaSparkContext sparkContext, String path, Map<ObjLvl, List<String>> requested) {
        super(sparkContext, path);
        this.requested = requested;
    }
}
