/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.config;

import io.github.pastorgl.datacooker.data.DataStream;

import java.util.Map;

public class NamedInput implements InputOutput {
    public final Map<String, DataStream> namedInputs;

    public  NamedInput(Map<String, DataStream> namedInputs) {
        this.namedInputs = namedInputs;
    }
}
