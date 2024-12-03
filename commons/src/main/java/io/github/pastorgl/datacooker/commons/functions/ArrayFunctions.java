/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function.ArbitrAry;
import io.github.pastorgl.datacooker.scripting.Function.Binary;
import io.github.pastorgl.datacooker.scripting.Function.Ternary;
import io.github.pastorgl.datacooker.scripting.Function.Unary;

import java.util.Arrays;
import java.util.Deque;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class ArrayFunctions {
    public static class Slice extends Ternary<Object, Object, Integer, Integer> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            return new ArrayWrap(Arrays.copyOfRange(a.data, Evaluator.popInt(args), Evaluator.popInt(args)));
        }

        @Override
        public String name() {
            return "ARR_SLICE";
        }

        @Override
        public String descr() {
            return "Return a slice of ARRAY given as 1st argument starting with index from 2nd and to index in 3rd (exclusive)";
        }
    }

    public static class Item extends Binary<Object, Object, Integer> {
        @Override
        public Object call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            return a.data[Evaluator.popInt(args)];
        }

        @Override
        public String name() {
            return "ARR_ITEM";
        }

        @Override
        public String descr() {
            return "Return an element of ARRAY given as 1st argument by index set in 2nd";
        }
    }

    public static class Length extends Unary<Integer, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            return a.data.length;
        }

        @Override
        public String name() {
            return "ARR_LENGTH";
        }

        @Override
        public String descr() {
            return "Returns the number of elements in the given ARRAY";
        }
    }

    public static class Join extends Binary<String, Object, String> {
        @Override
        public String call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            return Arrays.stream(a.data).map(String::valueOf).collect(Collectors.joining(Evaluator.popString(args)));
        }

        @Override
        public String name() {
            return "ARR_JOIN";
        }

        @Override
        public String descr() {
            return "Convert ARRAY given as 1st argument into String using a String delimiter given as 2nd";
        }
    }

    public static class Make extends ArbitrAry<Object, Object> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            return new ArrayWrap(args);
        }

        @Override
        public String name() {
            return "ARR_MAKE";
        }

        @Override
        public String descr() {
            return "Make an ARRAY from all arguments in their given order";
        }
    }
}
