/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.config;

import io.github.pastorgl.datacooker.math.functions.attrs.AttrsFunction;
import io.github.pastorgl.datacooker.math.functions.attrs.MaxFunction;
import io.github.pastorgl.datacooker.math.functions.attrs.MedianFunction;
import io.github.pastorgl.datacooker.math.functions.attrs.MinFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionEnum;

public enum AttrsMath implements DefinitionEnum {
    MIN("Find the minimal value among attributes, optionally with a set floor", MinFunction.class),
    MAX("Find the maximal value among attributes, optionally with a set ceiling", MaxFunction.class),
    MEDIAN("Calculate the median", MedianFunction.class);

    private final String descr;
    private final Class<? extends AttrsFunction> function;

    AttrsMath(String descr, Class<? extends AttrsFunction> function) {
        this.descr = descr;
        this.function = function;
    }

    @Override
    public String descr() {
        return descr;
    }

    public AttrsFunction function(String[] sourceColumns) throws Exception {
        return function.getConstructor(String[].class).newInstance(new Object[]{sourceColumns});
    }
}
