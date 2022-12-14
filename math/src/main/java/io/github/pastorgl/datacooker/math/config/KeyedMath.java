/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.config;

import io.github.pastorgl.datacooker.metadata.DefinitionEnum;
import io.github.pastorgl.datacooker.math.functions.keyed.*;

public enum KeyedMath implements DefinitionEnum {
    SUM("Calculate the sum of attributes, optionally add a constant", SumFunction.class),
    SUBTRACT("Subtract all subsequent attribute values from the first, optionally also a constant", SubtractFunction.class),
    AVERAGE("Calculate the arithmetic mean of attributes, optionally shifted towards a constant", AverageFunction.class),
    POWERMEAN("Calculate the power mean of attributes with a set power", PowerMeanFunction.class) {
        @Override
        public KeyedFunction function(Double power) throws Exception {
            if (power == null) {
                throw new NullPointerException("Power mean requires a power constant");
            }
            return super.function(power);
        }
    },
    RMS("Calculate the square root of the mean square (quadratic mean or RMS)", PowerMeanFunction.class) {
        @Override
        public KeyedFunction function(Double ignore) throws Exception {
            return super.function(2.D);
        }
    },
    MIN("Find the minimal value among attributes, optionally with a set floor", MinFunction.class),
    MAX("Find the maximal value among attributes, optionally with a set ceil", MaxFunction.class),
    MUL("Multiply column values, optionally also by a constant", MulFunction.class),
    DIV("Divide first attributes by all others, optionally also by a constant", DivFunction.class),
    EQUALITY("Check equality of all values, optionally within a threshold constant." +
            " Returns 1.0 if equal, or 0.0 otherwise", EqualityFunction.class),
    MEDIAN("Calculate the median. Constant is ignored", QuantileFunction.class) {
        @Override
        public KeyedFunction function(Double ignore) throws Exception {
            return super.function(0.5D);
        }
    },
    QUANTILE("Calculate the quantile. Constant is in the interval from 0.0 to 1.0 (exclusive)", QuantileFunction.class) {
        @Override
        public KeyedFunction function(Double quantile) throws Exception {
            if ((quantile == null) || (quantile <= 0.0D) || (quantile >= 1.0D)) {
                throw new IndexOutOfBoundsException("Quantile must be in the range from 0.0 to 1.0 (exclusive)");
            }
            return super.function(quantile);
        }
    };

    private final String descr;
    private final Class<? extends KeyedFunction> function;

    KeyedMath(String descr, Class<? extends KeyedFunction> function) {
        this.descr = descr;
        this.function = function;
    }

    @Override
    public String descr() {
        return descr;
    }

    public KeyedFunction function(Double _const) throws Exception {
        return function.getConstructor(Double.class).newInstance(_const);
    }
}
