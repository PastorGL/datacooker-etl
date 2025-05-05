/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.operation;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.NamedInput;
import io.github.pastorgl.datacooker.config.NamedOutput;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.metadata.Pluggable;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.Map;

public abstract class FullOperation extends Pluggable<NamedInput, NamedOutput> {
    protected Map<String, DataStream> inputStreams;
    protected Map<String, String> outputStreams;
    protected ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();

    public void configure(Configuration params) throws InvalidConfigurationException {
    }

    @Override
    final public void initialize(NamedInput input, NamedOutput output) throws InvalidConfigurationException {
        this.inputStreams = input.namedInputs;
        this.outputStreams = output.outputMap;
    }

    @Override
    final public Map<String, DataStream> result() {
        return outputs;
    }
}
