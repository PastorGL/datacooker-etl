/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker;

import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.metadata.FunctionInfo;
import io.github.pastorgl.datacooker.metadata.PluggableInfo;
import io.github.pastorgl.datacooker.scripting.OptionsContext;
import io.github.pastorgl.datacooker.scripting.Procedure;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import java.util.TreeMap;

public class DataCooker {
    public static DataContext DATA_CONTEXT;
    public static OptionsContext OPTIONS_CONTEXT;
    public static VariablesContext GLOBAL_VARS;

    public static Map<String, Procedure> PROCEDURES;
    public static Map<String, FunctionInfo> FUNCTIONS;
    public static Map<String, PluggableInfo> TRANSFORMS;

    public static void initialize(JavaSparkContext context, OptionsContext oc, DataContext dc, VariablesContext vc) {
        oc.initialize();
        OPTIONS_CONTEXT = oc;

        dc.initialize(context);
        DATA_CONTEXT = dc;

        vc.initialize();
        GLOBAL_VARS = vc;

        PROCEDURES = new TreeMap<>();
        FUNCTIONS = new TreeMap<>();
        TRANSFORMS = new TreeMap<>();
    }
}
