/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.config.InputOutput;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.metadata.Pluggable;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class StorageAdapter<I extends InputOutput, O extends InputOutput> extends Pluggable<I, O> {
    protected JavaSparkContext context;
    protected String path;

    public void configure(Configuration params) throws InvalidConfigurationException {
    }
}
