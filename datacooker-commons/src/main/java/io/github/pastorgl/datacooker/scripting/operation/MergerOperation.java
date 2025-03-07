/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.operation;

import io.github.pastorgl.datacooker.config.*;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.metadata.Pluggable;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.Map;

public abstract class MergerOperation extends Pluggable<NamedInput, Output> {
    protected Map<String, DataStream> inputStreams;
    protected String name;
    protected DataStream outputStream;

    public void configure(Configuration params) throws InvalidConfigurationException {
    }

    @Override
    public void initialize(NamedInput input, Output output) throws InvalidConfigurationException {
        this.inputStreams = input.namedInputs;

        this.name = output.name;
    }

    @Override
    public Map<String, DataStream> result() {
        return Map.of(name, outputStream);
    }
}
