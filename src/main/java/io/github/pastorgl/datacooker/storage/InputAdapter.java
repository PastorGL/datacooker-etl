/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;

import java.util.List;
import java.util.Map;

public abstract class InputAdapter extends StorageAdapter {
    public void configure(Map<String, Object> adapterConfig) throws InvalidConfigurationException {
        resolver = new Configuration(meta.definitions, "Input " + meta.verb, adapterConfig);

        configure();
    }

    public abstract List<DataHolder> load(String path) throws Exception;
}
