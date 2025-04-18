/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function.ArbitrAry;
import io.github.pastorgl.datacooker.scripting.Function.Binary;
import io.github.pastorgl.datacooker.scripting.Function.Ternary;
import io.github.pastorgl.datacooker.scripting.Function.Unary;

import java.util.Base64;
import java.util.Deque;
import java.util.Locale;

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

    public static class Split extends Ternary<ArrayWrap, String, String, Integer> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return new ArrayWrap(subject.split(Evaluator.popString(args), Evaluator.popInt(args)));
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

    public static class Lower extends Unary<String, String> {
        @Override
        public String call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.toLowerCase(Locale.ROOT);
        }

        @Override
        public String name() {
            return "STR_LOWER";
        }

        @Override
        public String descr() {
            return "Returns String converted to lower case (in no particular locale)";
        }
    }

    public static class Upper extends Unary<String, String> {
        @Override
        public String call(Deque<Object> args) {
            String subject = Evaluator.popString(args);
            return subject.toUpperCase(Locale.ROOT);
        }

        @Override
        public String name() {
            return "STR_UPPER";
        }

        @Override
        public String descr() {
            return "Returns String converted to upper case (in no particular locale)";
        }
    }

    public static class Starts extends Binary<Boolean, String, String> {
        @Override
        public Boolean call(Deque<Object> args) {
            String haystack = Evaluator.popString(args);
            String needle = Evaluator.popString(args);
            return haystack.startsWith(needle);
        }

        @Override
        public String name() {
            return "STR_STARTS";
        }

        @Override
        public String descr() {
            return "Returns TRUE if 1st arguments starts with 2nd, FALSE otherwise";
        }
    }

    public static class Ends extends Binary<Boolean, String, String> {
        @Override
        public Boolean call(Deque<Object> args) {
            String haystack = Evaluator.popString(args);
            String needle = Evaluator.popString(args);
            return haystack.endsWith(needle);
        }

        @Override
        public String name() {
            return "STR_ENDS";
        }

        @Override
        public String descr() {
            return "Returns TRUE if 1st arguments ends with 2nd, FALSE otherwise";
        }
    }

    public static class Contains extends Binary<Boolean, String, String> {
        @Override
        public Boolean call(Deque<Object> args) {
            String haystack = Evaluator.popString(args);
            String needle = Evaluator.popString(args);
            return haystack.contains(needle);
        }

        @Override
        public String name() {
            return "STR_CONTAINS";
        }

        @Override
        public String descr() {
            return "Returns TRUE if 1st arguments contains 2nd, FALSE otherwise";
        }
    }

    public static class Format extends ArbitrAry<String, Object> {
        @Override
        public String call(Deque<Object> args) {
            String format = Evaluator.popString(args);
            return String.format(Locale.ROOT, format, args.toArray());
        }

        @Override
        public String name() {
            return "STR_FORMAT";
        }

        @Override
        public String descr() {
            return "Format String using 1st argument as format specifier and any subsequent arguments as subjects." +
                    " See Java Formatter class for complete reference";
        }
    }

    public static class DECODE64 extends Unary<String, String> {
        @Override
        public String call(Deque<Object> args) {
            return new String(Base64.getDecoder().decode(Evaluator.popString(args)));
        }

        @Override
        public String name() {
            return "STR_DECODE64";
        }

        @Override
        public String descr() {
            return "Decode a BASE64 encoded string";
        }
    }

    public static class ENCODE64 extends Unary<String, Object> {
        @Override
        public String call(Deque<Object> args) {
            return Base64.getEncoder().encodeToString(Evaluator.popBytes(args));
        }

        @Override
        public String name() {
            return "STR_BASE64";
        }

        @Override
        public String descr() {
            return "Encode stringified Object or byte array to a BASE64 encoded string";
        }
    }
}
