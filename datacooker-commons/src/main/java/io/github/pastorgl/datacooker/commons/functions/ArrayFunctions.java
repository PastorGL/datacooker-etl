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
    public static class Slice extends Ternary<ArrayWrap, ArrayWrap, Integer, Integer> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            return new ArrayWrap(Arrays.copyOfRange(a.data(), Evaluator.popInt(args), Evaluator.popInt(args)));
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

    public static class Item extends Binary<Object, ArrayWrap, Integer> {
        @Override
        public Object call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            return a.get(Evaluator.popInt(args));
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

    public static class Put extends Ternary<Object, ArrayWrap, Integer, Object> {
        @Override
        public Object call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            int idx = Evaluator.popInt(args);
            return a.put(idx, args.pop());
        }

        @Override
        public String name() {
            return "ARR_PUT";
        }

        @Override
        public String descr() {
            return "Sets an element of ARRAY given as 1st argument by index set in 2nd to value from 3rd. Returns previous value";
        }
    }

    public static class Length extends Unary<Integer, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ArrayWrap a = Evaluator.popArray(args);
            return a.length();
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
            return Arrays.stream(a.data()).map(String::valueOf).collect(Collectors.joining(Evaluator.popString(args)));
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

    public static class Make extends ArbitrAry<ArrayWrap, Object> {
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

    public static class MakeRange extends Binary<ArrayWrap, Number, Number> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            Number a = Evaluator.popNumeric(args);
            Number b = Evaluator.popNumeric(args);

            return ArrayWrap.fromRange(a, b);
        }

        @Override
        public String name() {
            return "ARR_RANGE";
        }

        @Override
        public String descr() {
            return "Make a RANGE with both boundaries included, with order preserved. If both are Integer," +
                    " all RANGE values are Integer, and Long otherwise";
        }
    }

    public static class Insert extends Ternary<ArrayWrap, ArrayWrap, Integer, Object> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            ArrayWrap e = Evaluator.popArray(args);
            int idx = Evaluator.popInt(args);
            Object v = args.pop();

            Object[] e_data = e.data();
            Object[] data = new Object[e_data.length + 1];
            System.arraycopy(e_data, 0, data, 0, idx);
            data[idx] = v;
            System.arraycopy(e_data, idx, data, idx + 1, e_data.length - idx);
            return new ArrayWrap(data);
        }

        @Override
        public String name() {
            return "ARR_INSERT";
        }

        @Override
        public String descr() {
            return "Returns new ARRAY, made from ARRAY given as 1st argument, with a value inserted at index from 2nd" +
                    " argument that comes from 3rd. Shifts existing values right";
        }
    }

    public static class Add extends Binary<ArrayWrap, ArrayWrap, Object> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            ArrayWrap e = Evaluator.popArray(args);
            Object v = args.pop();

            Object[] e_data = e.data();
            Object[] data = new Object[e_data.length + 1];
            System.arraycopy(e_data, 0, data, 0, e_data.length);
            data[e_data.length] = v;
            return new ArrayWrap(data);
        }

        @Override
        public String name() {
            return "ARR_ADD";
        }

        @Override
        public String descr() {
            return "Returns new ARRAY, made from ARRAY given as 1st argument, with a value inserted at the end" +
                    " that comes from 2nd argument";
        }
    }

    public static class Fill extends Binary<ArrayWrap, Integer, Object> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            int l = Evaluator.popInt(args);
            Object v = args.pop();

            Object[] data = new Object[l];
            Arrays.fill(data, v);
            return new ArrayWrap(data);
        }

        @Override
        public String name() {
            return "ARR_FILL";
        }

        @Override
        public String descr() {
            return "Returns new ARRAY with length given as 1st argument," +
                    " filled with a value that comes from 2nd argument";
        }
    }
}
