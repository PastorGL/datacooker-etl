/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.scripting.Evaluator.Binary;
import io.github.pastorgl.datacooker.scripting.Evaluator.Ternary;
import io.github.pastorgl.datacooker.scripting.Evaluator.Unary;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;

import java.util.Arrays;
import java.util.Deque;

@SuppressWarnings("unused")
public class ArrayFunctions {
    public static class Slice extends Function implements Ternary {
        @Override
        public Object call(Deque<Object> args) {
            Object[] a = Evaluator.popArray(args);
            return Arrays.copyOfRange(a, Evaluator.popInt(args), Evaluator.popInt(args));
        }

        @Override
        public String name() {
            return "ARRAY_SLICE";
        }
    }

    public static class Item extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            Object[] a = Evaluator.popArray(args);
            return a[Evaluator.popInt(args)];
        }

        @Override
        public String name() {
            return "ARRAY_ITEM";
        }
    }

    public static class Length extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            Object[] a = Evaluator.popArray(args);
            return a.length;
        }

        @Override
        public String name() {
            return "ARRAY_LENGTH";
        }
    }
}
