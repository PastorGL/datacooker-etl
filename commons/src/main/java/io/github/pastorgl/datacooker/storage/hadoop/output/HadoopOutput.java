/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.storage.OutputAdapter;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.CODEC;

public abstract class HadoopOutput extends OutputAdapter {
    protected HadoopStorage.Codec codec;

    protected void configure() throws InvalidConfigurationException {
        codec = resolver.get(CODEC);
    }

    @Override
    public void save(String sub, DataStream ds) {
        OutputFunction outputFunction = getOutputFunction(sub);

        ds.rdd.mapPartitionsWithIndex(outputFunction, true).count();
    }

    abstract protected OutputFunction getOutputFunction(String sub);
}
