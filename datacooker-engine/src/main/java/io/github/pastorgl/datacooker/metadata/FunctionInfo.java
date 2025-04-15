/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.scripting.Function;

public class FunctionInfo extends EvaluatorInfo {
    public final Function<?> instance;

    public FunctionInfo(Function<?> function) {
        super(function);

        this.instance = function;
    }

    @JsonCreator
    public FunctionInfo(String symbol, String descr, int arity, String[] argTypes, String resultType) {
        super(symbol, descr, arity, argTypes, resultType);

        this.instance = null;
    }
}
