/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.config.InputOutput;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.data.DataStream;

import java.util.Map;

public abstract class Pluggable<I extends InputOutput, O extends InputOutput> {
    //1. instantiate and get meta once
    public abstract PluggableMeta meta();

    //2. pass parameters on each call, that may have multiple invocations
    public abstract void configure(Configuration params) throws InvalidConfigurationException;

    //3. initialize IO set before invocation
    public abstract void initialize(I input, O output) throws InvalidConfigurationException;

    //4. invoke for that IO set
    public abstract void execute() throws RuntimeException;

    //5. get last invocation's result if needed
    public abstract Map<String, DataStream> result();
}
