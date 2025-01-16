/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.Structured;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;

import java.util.Deque;

@SuppressWarnings("unused")
public class StructuredFunctions {
    public static class ATTRS extends Function.Unary<String[], Structured> {
        @Override
        public String[] call(Deque<Object> args) {
            return ((Structured) args.pop()).attrs().toArray(new String[0]);
        }

        @Override
        public String name() {
            return "STRUCT_ATTRS";
        }

        @Override
        public String descr() {
            return "Returns ARRAY of top-level attributes of a Structured Object";
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
}
