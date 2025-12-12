/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.Input;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.PathOutput;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.storage.OutputAdapter;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.CODEC;

public abstract class HadoopOutput extends OutputAdapter {
    protected HadoopStorage.Codec codec;
    protected DataStream ds;
    protected String[] columns;

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        codec = params.get(CODEC);
    }

    @Override
    public void initialize(Input input, PathOutput output) throws InvalidConfigurationException {
        super.initialize(input, output);

        this.ds = input.dataStream;
        this.columns = ((output.requested != null) && output.requested.containsKey(VALUE))
                ? output.requested.get(VALUE).toArray(new String[0])
                : ds.attributes(VALUE).toArray(new String[0]);
    }

    @Override
    public void execute() {
        OutputFunction outputFunction = getOutputFunction(ds.name);

        ds.rdd().mapPartitionsWithIndex(outputFunction, true).count();
    }

    abstract protected OutputFunction getOutputFunction(String sub);
}
