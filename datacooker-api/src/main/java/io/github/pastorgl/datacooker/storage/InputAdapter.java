/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Output;
import io.github.pastorgl.datacooker.config.PathInput;
import io.github.pastorgl.datacooker.data.Partitioning;

public abstract class InputAdapter extends StorageAdapter<PathInput, Output> {
    protected int partCount;
    protected Partitioning partitioning;

    @Override
    public void initialize(PathInput input, Output output) throws InvalidConfigurationException {
        this.context = input.sparkContext;
        this.path = input.path;
        this.partCount = input.partCount;
        this.partitioning = input.partitioning;
    }
}
