/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function.RecordKey;
import io.github.pastorgl.datacooker.scripting.Function.RecordObject;

import java.util.Deque;
import java.util.Objects;

@SuppressWarnings("unused")
public class RecordLevelFunctions {
    public static class HASHCODE extends RecordObject<Integer, DataRecord<?>> {
        @Override
        public Integer call(Deque<Object> args) {
            return Objects.hashCode(args.pop());
        }

        @Override
        public String name() {
            return "REC_HASHCODE";
        }

        @Override
        public String descr() {
            return "Returns Java .hashCode() of a DS Record";
        }
    }

    public static class ATTRS extends RecordObject<String[], DataRecord<?>> {
        @Override
        public String[] call(Deque<Object> args) {
            return ((DataRecord<?>) args.pop()).attrs().toArray(new String[0]);
        }

        @Override
        public String name() {
            return "REC_ATTRS";
        }

        @Override
        public String descr() {
            return "Returns ARRAY of top-level Attribute names of a DS Record";
        }
    }

    public static class KEY extends RecordKey<Object> {
        @Override
        public Object call(Deque<Object> args) {
            return args.pop();
        }

        @Override
        public String name() {
            return "REC_KEY";
        }

        @Override
        public String descr() {
            return "Returns the Key of a DS Record";
        }
    }

    public static class OBJECT extends RecordObject<DataRecord<?>, DataRecord<?>> {
        @Override
        public DataRecord<?> call(Deque<Object> args) {
            return (DataRecord<?>) args.pop();
        }

        @Override
        public String name() {
            return "REC_OBJECT";
        }

        @Override
        public String descr() {
            return "Returns the DS Record Object itself";
        }
    }

    public static class ATTR extends RecordObject<Object, DataRecord<?>> {
        @Override
        public Object call(Deque<Object> args) {
            return ((DataRecord<?>) args.pop()).asIs(Evaluator.popString(args));
        }

        @Override
        public String name() {
            return "REC_ATTR";
        }

        @Override
        public String descr() {
            return "Returns the specified Attribute value of a DS Record";
        }
    }

    public static class PIVOT extends RecordObject<ArrayWrap, DataRecord<?>> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            return new ArrayWrap(((DataRecord<?>) args.pop()).asIs().values());
        }

        @Override
        public String name() {
            return "REC_PIVOT";
        }

        @Override
        public String descr() {
            return "Returns top-level Attributes of a DS Record as an ARRAY";
        }
    }
}
