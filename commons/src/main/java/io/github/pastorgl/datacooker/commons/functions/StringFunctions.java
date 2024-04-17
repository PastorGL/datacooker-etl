/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function.Ternary;
import io.github.pastorgl.datacooker.scripting.Function.Unary;

import java.util.Deque;

@SuppressWarnings("unused")
public class StringFunctions {
    public static class Replace extends Ternary<String, String, String, String> {
        @Override
        public String call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.replaceAll(Evaluator.popString(args), Evaluator.popString(args));
        }

        @Override
        public String name() {
            return "STR_REPLACE";
        }

        @Override
        public String descr() {
            return "Replaces each substring of a String from the 1st argument that matches the given regular expression" +
                    " (2nd) with the given replacement (3rd). See Java String.replaceAll() method for complete reference";
        }
    }

    public static class Substr extends Ternary<String, String, Integer, Integer> {
        @Override
        public String call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.substring(Evaluator.popInt(args), Evaluator.popInt(args));
        }

        @Override
        public String name() {
            return "STR_SLICE";
        }

        @Override
        public String descr() {
            return "Returns a substring of the String given as 1st argument. The substring begins at the index from 2nd and" +
                    " extends to the index from 3rd argument (exclusive)";
        }
    }

    public static class Split extends Ternary<String[], String, String, Integer> {
        @Override
        public String[] call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.split(Evaluator.popString(args), Evaluator.popInt(args));
        }

        @Override
        public String name() {
            return "STR_SPLIT";
        }

        @Override
        public String descr() {
            return "Splits the String given as 1st argument around matches of the regular expression given as 2nd." +
                    " Max number of matches is set by 3rd argument. See Java String.split() method for complete reference";
        }
    }

    public static class Length extends Unary<Integer, String> {
        @Override
        public Integer call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.length();
        }

        @Override
        public String name() {
            return "STR_LENGTH";
        }

        @Override
        public String descr() {
            return "Returns String length";
        }
    }
}
