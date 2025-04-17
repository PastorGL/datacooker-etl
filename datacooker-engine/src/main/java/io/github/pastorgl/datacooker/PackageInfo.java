/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.metadata.FunctionInfo;
import io.github.pastorgl.datacooker.metadata.OperatorInfo;
import io.github.pastorgl.datacooker.metadata.PluggableInfo;

import java.io.Serializable;
import java.util.*;

public class PackageInfo implements Serializable {
    public final String descr;
    public final List<PluggableInfo> pluggables;
    public final Map<String, FunctionInfo> functions;
    public final Map<String, OperatorInfo> operators;

    public PackageInfo(String descr) {
        this.descr = descr;

        this.pluggables = new ArrayList<>();
        this.functions = new TreeMap<>();
        this.operators = new LinkedHashMap<>();
    }

    @JsonCreator
    public PackageInfo(String descr, List<PluggableInfo> pluggables, Map<String, FunctionInfo> functions, Map<String, OperatorInfo> operators) {
        this.descr = descr;
        this.pluggables = pluggables;
        this.functions = functions;
        this.operators = operators;
    }
}
