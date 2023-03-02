/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;

import java.util.Map;

public abstract class OutputAdapter extends StorageAdapter {
    public void configure(Map<String, Object> adapterConfig) throws InvalidConfigurationException {
        resolver = new Configuration(meta.definitions, "Output " + meta.verb, adapterConfig);

        configure();
    }

    public abstract void save(String path, DataHolder rdd);
}
