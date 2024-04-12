/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.scripting.Function.RecordLevel;

import java.util.Deque;
import java.util.Objects;

@SuppressWarnings("unused")
public class RecordLevelFunctions {
    public static class HASHCODE extends RecordLevel<Integer, Record<?>> {
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
}
