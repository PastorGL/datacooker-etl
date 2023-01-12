/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.dist.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.AdapterResolver;
import io.github.pastorgl.datacooker.metadata.DataHolder;

import java.util.List;
import java.util.Map;

public abstract class InputAdapter extends StorageAdapter {
    public void configure(Map<String, Object> adapterConfig) throws InvalidConfigurationException {
        resolver = new AdapterResolver(meta, adapterConfig);

        configure();
    }

    public abstract List<DataHolder> load(String path) throws Exception;
}
