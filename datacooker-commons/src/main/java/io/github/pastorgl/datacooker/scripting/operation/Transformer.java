/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.operation;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.Input;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Output;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.metadata.Pluggable;

import java.util.List;
import java.util.Map;

public abstract class Transformer extends Pluggable<Input, Output> {
    private Configuration params;

    private DataStream inputStream;

    private DataStream outputStream;
    private Map<ObjLvl, List<String>> newColumns;

    protected String outputName;

    @Override
    final public void configure(Configuration params) throws InvalidConfigurationException {
        this.params = params;
    }

    @Override
    final public void initialize(Input input, Output output) throws InvalidConfigurationException {
        this.inputStream = input.dataStream;
        this.newColumns = output.requested;
        this.outputName = (output.name != null) ? output.name : inputStream.name;
    }

    @Override
    final public void execute() throws RuntimeException {
        outputStream = transformer().apply(inputStream, newColumns, params);
    }

    @Override
    final public Map<String, DataStream> result() {
        return Map.of(outputName, outputStream);
    }

    protected abstract StreamTransformer transformer();
}
