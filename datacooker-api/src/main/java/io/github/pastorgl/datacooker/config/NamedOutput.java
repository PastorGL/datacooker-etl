/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.config;

import org.apache.commons.collections4.map.ListOrderedMap;

public class NamedOutput implements InputOutput {
    public final ListOrderedMap<String, String> outputMap;

    public NamedOutput(ListOrderedMap<String, String> outputMap) {
        this.outputMap = outputMap;
    }
}
