/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;

import java.util.Arrays;
import java.util.Deque;

@SuppressWarnings("unused")
public class ColumnarFunctions {
    public static class MakeColumnar extends Function.Binary<Columnar, ArrayWrap, ArrayWrap> {
        @Override
        public Columnar call(Deque<Object> args) {
            ArrayWrap keys = Evaluator.popArray(args);
            ArrayWrap values = Evaluator.popArray(args);
            return new Columnar(Arrays.stream(keys.data()).map(String::valueOf).toList(), values.data());
        }

        @Override
        public String name() {
            return "COLUMNAR_MAKE";
        }

        @Override
        public String descr() {
            return "Create a Columnar object using first array as names of columns, second as their values";
        }
    }
}
