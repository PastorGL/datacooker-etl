/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.dist.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.AdapterResolver;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class StorageAdapter {
    public final AdapterMeta meta;

    protected JavaSparkContext context;

    protected AdapterResolver resolver;

    public StorageAdapter() {
        this.meta = meta();
    }

    public void initialize(JavaSparkContext ctx) {
        this.context = ctx;
    }

    protected abstract AdapterMeta meta();

    abstract protected void configure() throws InvalidConfigurationException;
}
