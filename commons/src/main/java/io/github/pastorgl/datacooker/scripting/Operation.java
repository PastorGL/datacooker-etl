/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.metadata.Configurable;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.Map;

public abstract class Operation implements Configurable<OperationMeta> {
    public final OperationMeta meta;

    protected ListOrderedMap<String, DataStream> inputStreams;
    protected Configuration params;
    protected ListOrderedMap<String, String> outputStreams;

    public Operation() {
        this.meta = meta();
    }

    public void initialize(ListOrderedMap<String, DataStream> input, Configuration params, ListOrderedMap<String, String> output) throws InvalidConfigurationException {
        this.inputStreams = input;
        this.params = params;
        this.outputStreams = output;

        configure();
    }

    abstract protected void configure() throws InvalidConfigurationException;

    abstract public Map<String, DataStream> execute() throws InvalidConfigurationException;
}
