/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.config;

import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.spark.api.java.JavaSparkContext;

public class PathInput extends PathInputOutput {
    public final int partCount;
    public final Partitioning partitioning;
    public final boolean wildcard;

    public PathInput(JavaSparkContext sparkContext, String path, boolean wildcard, int partCount, Partitioning partitioning) {
        super(sparkContext, path);
        this.wildcard = wildcard;
        this.partCount = partCount;
        this.partitioning = partitioning;
    }
}
