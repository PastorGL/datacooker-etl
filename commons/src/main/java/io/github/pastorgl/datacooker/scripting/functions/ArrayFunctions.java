/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.functions;

import io.github.pastorgl.datacooker.scripting.Function;
import io.github.pastorgl.datacooker.scripting.Function.Binary;
import io.github.pastorgl.datacooker.scripting.Function.Unary;
import io.github.pastorgl.datacooker.scripting.Operator;

import java.util.Arrays;
import java.util.Deque;

@SuppressWarnings("unused")
public class ArrayFunctions {
    public static class Slice extends Function {
        @Override
        public Object call(Deque<Object> args) {
            Object[] a = Operator.popArray(args);
            return Arrays.copyOfRange(a, Operator.popInt(args), Operator.popInt(args));
        }

        @Override
        public String name() {
            return "ARRAY_SLICE";
        }

        @Override
        public int ariness() {
            return 3;
        }
    }

    public static class Item extends Binary {
        @Override
        public Object call(Deque<Object> args) {
            Object[] a = Operator.popArray(args);
            return a[Operator.popInt(args)];
        }

        @Override
        public String name() {
            return "ARRAY_ITEM";
        }
    }

    public static class Length extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            Object[] a = Operator.popArray(args);
            return a.length;
        }

        @Override
        public String name() {
            return "ARRAY_LENGTH";
        }
    }
}
