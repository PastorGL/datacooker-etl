/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function.RecordKey;
import io.github.pastorgl.datacooker.scripting.Function.RecordLevel;
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
            return "Returns Java .hashCode() of DS Record";
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
            return "Returns ARRAY of top-level attributes of DS Record";
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
            return "Returns the Key of DS Record";
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
            return "Returns the specified Attribute of DS Record as is";
        }
    }

    public static class PIVOT extends RecordLevel<Object[]> {
        @Override
        public Object[] call(Deque<Object> args) {
            return Evaluator.popArray(args).data();
        }

        @Override
        public String name() {
            return "PIVOT";
        }

        @Override
        public String descr() {
            return "Creates top-level Attributes of DS Record from ARRAY passed as an argument suffixed with ARRAY" +
                    " element indices";
        }
    }
}
