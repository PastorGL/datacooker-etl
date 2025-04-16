/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker;

import io.github.pastorgl.datacooker.metadata.FunctionInfo;
import io.github.pastorgl.datacooker.metadata.OperatorInfo;
import io.github.pastorgl.datacooker.metadata.PluggableInfo;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class PackageInfo {
    public final String descr;
    public final Map<String, PluggableInfo> pluggables = new TreeMap<>();
    public final Map<String, FunctionInfo> functions = new TreeMap<>();
    public final Map<String, OperatorInfo> operators = new LinkedHashMap<>();

    public PackageInfo(String descr) {
        this.descr = descr;
    }
}
