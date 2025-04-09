/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.operation;

import io.github.pastorgl.datacooker.config.*;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.metadata.Pluggable;

import java.util.Map;

public abstract class MergerOperation extends Pluggable<NamedInput, Output> {
    protected Map<String, DataStream> inputStreams;
    protected String outputName;

    private DataStream outputStream;

    public void configure(Configuration params) throws InvalidConfigurationException {
    }

    @Override
    final public void initialize(NamedInput input, Output output) throws InvalidConfigurationException {
        this.inputStreams = input.namedInputs;

        this.outputName = output.name;
    }

    protected abstract DataStream merge() throws RuntimeException;

    @Override
    final public void execute() throws RuntimeException {
        outputStream = merge();
    }

    @Override
    final public Map<String, DataStream> result() {
        return Map.of(outputName, outputStream);
    }
}
