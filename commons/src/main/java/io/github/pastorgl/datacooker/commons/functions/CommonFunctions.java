/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.scripting.Function;

import java.util.Deque;
import java.util.Objects;

@SuppressWarnings("unused")
public class CommonFunctions {
    public static class HASHCODE extends Function.Unary<Integer, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            return Objects.hashCode(args.pop());
        }

        @Override
        public String name() {
            return "OBJ_HASHCODE";
        }

        @Override
        public String descr() {
            return "Returns Java .hashCode() of any passed object or 0 if NULL";
        }
    }
}
