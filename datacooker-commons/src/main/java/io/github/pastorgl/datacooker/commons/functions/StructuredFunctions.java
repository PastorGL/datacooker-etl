/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.Structured;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;

import java.util.Deque;

@SuppressWarnings("unused")
public class StructuredFunctions {
    public static class ATTRS extends Function.Unary<ArrayWrap, Structured> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            return new ArrayWrap(((Structured) args.pop()).attrs());
        }

        @Override
        public String name() {
            return "STRUCT_ATTRS";
        }

        @Override
        public String descr() {
            return "Returns ARRAY of top-level attributes of a Structured Object (in no particular order)";
        }
    }

    public static class VALUES extends Function.Unary<ArrayWrap, Structured> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            return new ArrayWrap(((Structured) args.pop()).asIs().values());
        }

        @Override
        public String name() {
            return "STRUCT_VALUES";
        }

        @Override
        public String descr() {
            return "Returns ARRAY of top-level attribute values of a Structured Object (in no particular order)";
        }
    }

    public static class ATTR extends Function.Binary<Object, Structured, String> {
        @Override
        public Object call(Deque<Object> args) {
            return ((Structured) args.pop()).asIs(Evaluator.popString(args));
        }

        @Override
        public String name() {
            return "STRUCT_ATTR";
        }

        @Override
        public String descr() {
            return "Returns the specified Attribute of a Structured Object as is";
        }
    }

    public static class FromArray extends Function.Binary<Structured, ArrayWrap, Boolean> {
        @Override
        public Structured call(Deque<Object> args) {
            ArrayWrap arr = Evaluator.popArray(args);
            boolean conv = Evaluator.popBoolean(args);
            Structured ret = new Structured();
            Object[] data = arr.data();
            if (conv) {
                for (int i = 0; i < arr.size(); i++) {
                    ret.put(String.valueOf(data[i]), i);
                }
            } else {
                for (int i = 0; i < arr.size(); i++) {
                    ret.put(String.valueOf(i), data[i]);
                }
            }
            return ret;
        }

        @Override
        public String name() {
            return "STRUCT_FROM_ARRAY";
        }

        @Override
        public String descr() {
            return "Creates a Structured Object from the Array given as 1st argument. If 2nd argument is TRUE," +
                    " array values become property names, and indices become property values. If FALSE, array indices" +
                    " become property names and array values become property values";
        }
    }
}
