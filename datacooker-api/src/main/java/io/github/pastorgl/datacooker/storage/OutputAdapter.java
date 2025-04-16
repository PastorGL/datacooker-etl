/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.config.Input;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.PathOutput;
import io.github.pastorgl.datacooker.data.DataStream;

import java.util.Map;

public abstract class OutputAdapter extends StorageAdapter<Input, PathOutput> {
    @Override
    public void initialize(Input input, PathOutput output) throws InvalidConfigurationException {
        this.context = output.sparkContext;
        this.path = output.path;
    }

    @Override
    public Map<String, DataStream> result() {
        return null;
    }
}
