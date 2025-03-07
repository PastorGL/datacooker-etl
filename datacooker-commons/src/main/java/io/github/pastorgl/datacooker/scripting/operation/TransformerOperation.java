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
import io.github.pastorgl.datacooker.metadata.Pluggable;

import java.util.Map;

public abstract class TransformerOperation extends Pluggable<Input, Output> {
    private DataStream inputStream;

    private DataStream outputStream;
    private String outputName;

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
    }

    @Override
    public void initialize(Input input, Output output) throws InvalidConfigurationException {
        this.inputStream = input.dataStream;
        this.outputName = output.name;
    }

    @Override
    public void execute() throws RuntimeException {
        outputStream = transformer().apply(inputStream, outputName);
    }

    @Override
    public Map<String, DataStream> result() {
        return Map.of(outputName, outputStream);
    }

    abstract protected StreamTransformer transformer();
}
