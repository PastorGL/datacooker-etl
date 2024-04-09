/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.scripting.Function;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Evaluator.Ternary;

import java.util.Deque;

@SuppressWarnings("unused")
public class StringFunctions {
    public static class Replace extends Function implements Ternary {
        @Override
        public Object call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.replaceAll(Evaluator.popString(args), Evaluator.popString(args));
        }

        @Override
        public String name() {
            return "STR_REPLACE";
        }
    }

    public static class Substr extends Function implements Ternary {
        @Override
        public Object call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.substring(Evaluator.popInt(args), Evaluator.popInt(args));
        }

        @Override
        public String name() {
            return "STR_SLICE";
        }
    }

    public static class Split extends Function implements Ternary {
        @Override
        public Object call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.split(Evaluator.popString(args), Evaluator.popInt(args));
        }

        @Override
        public String name() {
            return "STR_SPLIT";
        }
    }
}
