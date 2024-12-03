/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.scripting.Evaluator;
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
public class ExpressionOperators {
    public static class TERNARY1 extends Binary<Object, Object, Object> {
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

    public static class TERNARY2 extends Binary<Object, Object, Object> {
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

    public static class OR extends Binary<Boolean, Boolean, Boolean> {
        @Override
        public int prio() {
            return 10;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            boolean y = Evaluator.peekNull(args);
            boolean a = !y && Evaluator.popBoolean(args);
            boolean z = Evaluator.peekNull(args);
            boolean b = !z && Evaluator.popBoolean(args);
            return (y && z) ? null : (a || b);
        }

        @Override
        public String name() {
            return "OR";
        }

        @Override
        public String descr() {
            return "Boolean OR";
        }

        @Override
        public boolean handleNull() {
            return true;
        }
    }

    public static class XOR extends Binary<Boolean, Boolean, Boolean> {
        @Override
        public int prio() {
            return 10;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            boolean y = Evaluator.peekNull(args);
            boolean a = !y && Evaluator.popBoolean(args);
            boolean z = Evaluator.peekNull(args);
            boolean b = !z && Evaluator.popBoolean(args);
            return (y && z) ? null : (a != b);
        }

        @Override
        public String name() {
            return "XOR";
        }

        @Override
        public String descr() {
            return "Boolean exclusive OR";
        }

        @Override
        public boolean handleNull() {
            return true;
        }
    }

    public static class AND extends Binary<Boolean, Boolean, Boolean> {
        @Override
        public int prio() {
            return 20;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            boolean a = Evaluator.popBoolean(args);
            boolean b = Evaluator.popBoolean(args);
            return a && b;
        }

        @Override
        public String name() {
            return "AND";
        }

        @Override
        public String descr() {
            return "Boolean AND";
        }
    }

    public static class NOT extends Unary<Boolean, Boolean> {
        @Override
        public int prio() {
            return 30;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            boolean a = Evaluator.peekNull(args);
            return a ? null : !Evaluator.popBoolean(args);
        }

        @Override
        public String name() {
            return "NOT";
        }

        @Override
        public String descr() {
            return "Boolean NOT";
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

    public static class GE extends Binary<Boolean, Double, Double> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return ">=";
        }

        @Override
        public String descr() {
            return "Numeric check for greater or equal";
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a >= b;
        }
    }

    public static class GT extends Binary<Boolean, Double, Double> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return ">";
        }

        @Override
        public String descr() {
            return "Numeric check for greater";
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a > b;
        }
    }

    public static class LE extends Binary<Boolean, Double, Double> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "<=";
        }

        @Override
        public String descr() {
            return "Numeric check for less or equal";
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a <= b;
        }
    }

    public static class LT extends Binary<Boolean, Double, Double> {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "<";
        }

        @Override
        public String descr() {
            return "Numeric check for less";
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a < b;
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

    public static class BOR extends Binary<Long, Long, Long> {
        @Override
        public int prio() {
            return 105;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "|";
        }

        @Override
        public String descr() {
            return "Bitwise OR";
        }

        @Override
        protected Long op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a | b;
        }
    }

    public static class BXOR extends Binary<Long, Long, Long> {
        @Override
        public int prio() {
            return 110;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "#";
        }

        @Override
        public String descr() {
            return "Bitwise exclusive OR";
        }

        @Override
        protected Long op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a ^ b;
        }
    }

    public static class BAND extends Binary<Long, Long, Long> {
        @Override
        public int prio() {
            return 115;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "&";
        }

        @Override
        public String descr() {
            return "Bitwise AND";
        }

        @Override
        protected Long op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a & b;
        }
    }

    public static class BSL extends Binary<Long, Long, Long> {
        @Override
        public int prio() {
            return 120;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "<<";
        }

        @Override
        public String descr() {
            return "Left shift";
        }

        @Override
        protected Long op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a << b;
        }
    }

    public static class BSR extends Binary<Long, Long, Long> {
        @Override
        public int prio() {
            return 120;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return ">>";
        }

        @Override
        public String descr() {
            return "Right shift";
        }

        @Override
        protected Long op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a >> b;
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

    public static class ADD extends Binary<Double, Double, Double> {
        @Override
        public int prio() {
            return 125;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "+";
        }

        @Override
        public String descr() {
            return "Addition";
        }

        @Override
        protected Double op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a + b;
        }
    }

    public static class SUB extends Binary<Double, Double, Double> {
        @Override
        public int prio() {
            return 125;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "-";
        }

        @Override
        public String descr() {
            return "Subtraction";
        }

        @Override
        protected Double op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a - b;
        }
    }

    public static class MUL extends Binary<Double, Double, Double> {
        @Override
        public int prio() {
            return 130;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "*";
        }

        @Override
        public String descr() {
            return "Multiplication";
        }

        @Override
        protected Double op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a * b;
        }
    }

    public static class DIV extends Binary<Double, Double, Double> {
        @Override
        public int prio() {
            return 130;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "/";
        }

        @Override
        public String descr() {
            return "Division";
        }

        @Override
        protected Double op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a / b;
        }
    }

    public static class MOD extends Binary<Double, Double, Double> {
        @Override
        public int prio() {
            return 130;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public String name() {
            return "%";
        }

        @Override
        public String descr() {
            return "Modulo";
        }

        @Override
        protected Double op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a % b;
        }
    }

    public static class EXP extends Binary<Double, Double, Double> {
        @Override
        public int prio() {
            return 135;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "^";
        }

        @Override
        public String descr() {
            return "Exponentiation";
        }

        @Override
        protected Double op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return Math.pow(a, b);
        }
    }

    public static class BNOT extends Unary<Long, Long> {
        @Override
        public int prio() {
            return 140;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        protected Long op0(Deque<Object> args) {
            return ~Evaluator.popLong(args);
        }

        @Override
        public String name() {
            return "~";
        }

        @Override
        public String descr() {
            return "Bitwise negation";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }
}
