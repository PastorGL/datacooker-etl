/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.operation;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.Input;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Output;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.data.StreamConverter;
import io.github.pastorgl.datacooker.data.Transform;
import io.github.pastorgl.datacooker.metadata.Pluggable;
import io.github.pastorgl.datacooker.metadata.Pluggables;

import java.util.List;
import java.util.Map;

public abstract class TransformCaller extends Pluggable<Input, Output> {
    private Configuration params;

    private DataStream inputStream;
    private String outputName;
    private Map<ObjLvl, List<String>> newColumns;

    private DataStream outputStream;

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        this.params = params;
    }

    @Override
    public void initialize(Input input, Output output) throws InvalidConfigurationException {
        this.inputStream = input.dataStream;
        this.outputName = output.name;
        this.newColumns = output.requested;
    }

    @Override
    public void execute() throws InvalidConfigurationException {
        try {
            StreamConverter converter = ((Transform) Pluggables.TRANSFORMS.get(meta().verb).pClass.getDeclaredConstructor().newInstance()).converter();

            outputStream = converter.apply(inputStream, newColumns, params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, DataStream> result() {
        return Map.of(outputName, outputStream);
    }
}
