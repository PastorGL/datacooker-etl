/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.functions;

import io.github.pastorgl.datacooker.scripting.Function;
import io.github.pastorgl.datacooker.scripting.Operator;

import java.util.Deque;

@SuppressWarnings("unused")
public class StringFunctions {
    public static class Replace extends Function {
        @Override
        public Object call(Deque<Object> args) {
            String subject = Operator.popString(args);
            return subject.replaceAll(Operator.popString(args), Operator.popString(args));
        }

        @Override
        public String name() {
            return "STR_REPLACE";
        }

        @Override
        public int ariness() {
            return 3;
        }
    }

    public static class Substr extends Function {
        @Override
        public Object call(Deque<Object> args) {
            String subject = Operator.popString(args);
            return subject.substring(Operator.popInt(args), Operator.popInt(args));
        }

        @Override
        public String name() {
            return "STR_SLICE";
        }

        @Override
        public int ariness() {
            return 3;
        }
    }

    public static class Split extends Function {
        @Override
        public Object call(Deque<Object> args) {
            String subject = Operator.popString(args);
            return subject.split(Operator.popString(args), Operator.popInt(args));
        }

        @Override
        public String name() {
            return "STR_SPLIT";
        }

        @Override
        public int ariness() {
            return 3;
        }
    }
}
