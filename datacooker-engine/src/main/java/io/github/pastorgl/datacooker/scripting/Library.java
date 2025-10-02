/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.metadata.FunctionInfo;
import io.github.pastorgl.datacooker.metadata.PluggableInfo;

import java.util.Map;
import java.util.TreeMap;

public class Library {
    public static Map<String, Procedure> PROCEDURES;
    public static Map<String, FunctionInfo> FUNCTIONS;
    public static Map<String, PluggableInfo> TRANSFORMS;

    public static void initialize() {
        PROCEDURES = new TreeMap<>();
        FUNCTIONS = new TreeMap<>();
        TRANSFORMS = new TreeMap<>();
    }
}
