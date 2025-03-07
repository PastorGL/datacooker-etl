/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.config;

import org.apache.spark.api.java.JavaSparkContext;

public class PathsInputOutput implements InputOutput {
    public final JavaSparkContext sparkContext;
    public final String path;

    public PathsInputOutput(JavaSparkContext sparkContext, String path) {
        this.sparkContext = sparkContext;
        this.path = path;
    }
}
