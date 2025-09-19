/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;

import java.util.Deque;

@SuppressWarnings("unused")
public class DataRecordFunctions {
    public static class ATTRS extends Function.Unary<String[], DataRecord<?>> {
        @Override
        public String[] call(Deque<Object> args) {
            return ((DataRecord<?>) args.pop()).attrs().toArray(new String[0]);
        }

        @Override
        public String name() {
            return "ATTRS";
        }

        @Override
        public String descr() {
            return "Returns ARRAY of top-level attribute names of any Record Object";
        }
    }

    public static class ATTR extends Function.Binary<Object, DataRecord<?>, String> {
        @Override
        public Object call(Deque<Object> args) {
            return ((DataRecord<?>) args.pop()).asIs(Evaluator.popString(args));
        }

        @Override
        public String name() {
            return "ATTR";
        }

        @Override
        public String descr() {
            return "For a Record Object, returns the values of a specified Attribute";
        }
    }

    public static class ATTR_REMOVE extends Function.Binary<DataRecord<?>, DataRecord<?>, String> {
        @Override
        public DataRecord<?> call(Deque<Object> args) {
            DataRecord<?> obj = (DataRecord<?>) args.pop();
            obj.remove(Evaluator.popString(args));
            return obj;
        }

        @Override
        public String name() {
            return "ATTR_REMOVE";
        }

        @Override
        public String descr() {
            return "Take a Record Object, and return it without the specified Attribute";
        }
    }

    public static class ATTR_PUT extends Function.Ternary<DataRecord<?>, DataRecord<?>, String, Object> {
        @Override
        public DataRecord<?> call(Deque<Object> args) {
            DataRecord<?> obj = (DataRecord<?>) args.pop();
            obj.put(Evaluator.popString(args), args.pop());
            return obj;
        }

        @Override
        public String name() {
            return "ATTR_PUT";
        }

        @Override
        public String descr() {
            return "Take a Record Object from 1st argument, and return it with the new Attribute with name given as" +
                    " 2nd argument and value as 3rd";
        }
    }
}
