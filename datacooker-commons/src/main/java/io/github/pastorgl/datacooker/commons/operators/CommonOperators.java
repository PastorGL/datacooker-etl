/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operators;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Operator;
import io.github.pastorgl.datacooker.scripting.Operator.Binary;
import io.github.pastorgl.datacooker.scripting.Operator.Unary;
import io.github.pastorgl.datacooker.scripting.Utils;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class CommonOperators {
    public static class TERNARY1 extends Operator.Binary<Object, Object, Object> {
        @Override
        public int prio() {
            return 0;
        }

        @Override
        protected Object op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            return (a == null) ? b : a;
        }

        @Override
        public String name() {
            return ":";
        }

        @Override
        public String descr() {
            return "Together with ? this is a ternary operator. If left argument is NULL, returns right, otherwise left";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public boolean handleNull() {
            return true;
        }
    }

    public static class DEFAULT extends TERNARY1 {
        @Override
        public String name() {
            return "DEFAULT";
        }

        @Override
        public String descr() {
            return "Alias of :";
        }
    }

    public static class TERNARY2 extends Operator.Binary<Object, Object, Object> {
        @Override
        public int prio() {
            return 5;
        }

        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = Evaluator.popBoolean(args);
            Object b = args.pop();
            return a ? b : null;
        }

        @Override
        public String name() {
            return "?";
        }

        @Override
        public String descr() {
            return "Together with : this is a ternary operator. If left argument is TRUE, returns right, otherwise NULL";
        }
    }

    public static class EQ extends Binary<Boolean, Object, Object> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return "=";
        }

        @Override
        public String descr() {
            return "Checks for exact equality. String checks are case-sensitive";
        }

        @Override
        public boolean handleNull() {
            return true;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            if ((a == null) || (b == null)) {
                return false;
            }
            if (a instanceof Number) {
                return ((Number) a).doubleValue() == Utils.parseNumber(String.valueOf(b)).doubleValue();
            }
            if (a instanceof Boolean) {
                return (boolean) a == Boolean.parseBoolean(String.valueOf(b));
            }
            if ((a instanceof ArrayWrap) || a.getClass().isArray() || (a instanceof Collection)) {
                return new ArrayWrap(a).equals(new ArrayWrap(b));
            }
            return Objects.equals(a, b);
        }
    }

    public static class EQ2 extends EQ {
        @Override
        public String name() {
            return "==";
        }

        @Override
        public boolean handleNull() {
            return false;
        }
    }

    public static class NEQ extends Binary<Boolean, Object, Object> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return "!=";
        }

        @Override
        public String descr() {
            return "Checks for inequality. String checks are case-sensitive";
        }

        @Override
        public boolean handleNull() {
            return true;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            if ((a == null) || (b == null)) {
                return false;
            }
            if (a instanceof Number) {
                return ((Number) a).doubleValue() != Utils.parseNumber(String.valueOf(b)).doubleValue();
            }
            if (a instanceof Boolean) {
                return (boolean) a != Boolean.parseBoolean(String.valueOf(b));
            }
            if ((a instanceof ArrayWrap) || a.getClass().isArray() || (a instanceof Collection)) {
                return !new ArrayWrap(a).equals(new ArrayWrap(b));
            }
            return !Objects.equals(a, b);
        }
    }

    public static class NE2 extends NEQ {
        @Override
        public String name() {
            return "<>";
        }

        @Override
        public boolean handleNull() {
            return false;
        }
    }

    public static class LIKE extends Binary<Boolean, String, String> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "LIKE";
        }

        @Override
        public String descr() {
            return "Left argument is checked to match to right argument, which is parsed as Java regexp";
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            String r = Evaluator.popString(args);

            String pattern = Evaluator.popString(args);
            int regexFlags = Pattern.MULTILINE;
            if (pattern.startsWith("/")) {
                int lastSlash = pattern.lastIndexOf('/');

                String patternFlags = pattern.substring(lastSlash).toLowerCase();
                regexFlags |= patternFlags.contains("i") ? Pattern.CASE_INSENSITIVE : 0;
                regexFlags |= patternFlags.contains("e") ? Pattern.DOTALL : 0;
                if (patternFlags.contains("s")) {
                    regexFlags &= ~Pattern.DOTALL;
                } else {
                    regexFlags |= Pattern.DOTALL;
                }

                pattern = pattern.substring(1, lastSlash);
            }

            final Pattern p = Pattern.compile(pattern, regexFlags);

            return (r != null) && p.matcher(r).matches();
        }
    }

    public static class MATCH extends LIKE {
        @Override
        public String descr() {
            return "Alias of LIKE";
        }

        @Override
        public String name() {
            return "MATCH";
        }
    }

    public static class REGEX extends LIKE {
        @Override
        public String descr() {
            return "Alias of LIKE";
        }

        @Override
        public String name() {
            return "REGEX";
        }
    }

    public static class DIGEST extends Binary<String, Object, String> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "DIGEST";
        }

        @Override
        public String descr() {
            return "Right argument is treated as Java digest provider in the format of provider-whitespace-algorithm" +
                    " or only algorithm, and left argument's hash is calculated and converted to hexadecimal string";
        }

        @Override
        protected String op0(Deque<Object> args) {
            String r = Evaluator.popString(args);

            String digest = Evaluator.popString(args);

            final String[] d = digest.split(" ", 2);

            MessageDigest md;
            try {
                md = (d.length > 1) ? MessageDigest.getInstance(d[1], d[0]) : MessageDigest.getInstance(d[0]);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Unknown DIGEST algorithm '" + digest + "'");
            }
            return Hex.encodeHexString(md.digest(r.getBytes()));
        }
    }

    public static class HASH extends DIGEST {
        @Override
        public String descr() {
            return "Alias of DIGEST";
        }

        @Override
        public String name() {
            return "HASH";
        }
    }

    public static class CAT extends Binary<String, Object, Object> {
        @Override
        public int prio() {
            return 125;
        }

        @Override
        public String name() {
            return "||";
        }

        @Override
        public String descr() {
            return "String concatenation";
        }

        @Override
        protected String op0(Deque<Object> args) {
            return args.stream().map(String::valueOf).collect(Collectors.joining());
        }
    }

    public static class HASHCODE extends Unary<Integer, Object> {
        @Override
        public boolean handleNull() {
            return true;
        }

        @Override
        public int prio() {
            return 145;
        }

        @Override
        public Integer op0(Deque<Object> args) {
            return Objects.hashCode(args.pop());
        }

        @Override
        public String name() {
            return "HASHCODE";
        }

        @Override
        public String descr() {
            return "Returns Java .hashCode() of any object or 0 if NULL";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }
}
