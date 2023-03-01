/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.Configurable;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class StorageAdapter implements Configurable<AdapterMeta> {
    public final AdapterMeta meta;

    protected JavaSparkContext context;

    protected Configuration resolver;

    public StorageAdapter() {
        this.meta = meta();
    }

    public void initialize(JavaSparkContext ctx) {
        this.context = ctx;
    }

    abstract protected void configure() throws InvalidConfigurationException;
}
