/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.Configurable;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class StorageAdapter<C extends AdapterMeta> implements Configurable<C> {
    public final C meta;

    protected JavaSparkContext context;
    protected String path;

    public StorageAdapter() {
        this.meta = meta();
    }

    public void initialize(JavaSparkContext sparkContext, Configuration params, String path) throws InvalidConfigurationException {
        context = sparkContext;
        this.path = path;

        configure(params);
    }

    protected void configure(Configuration params) throws InvalidConfigurationException {
    }
}
