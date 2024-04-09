/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Evaluator.Binary;
import io.github.pastorgl.datacooker.scripting.Evaluator.Unary;
import io.github.pastorgl.datacooker.scripting.Operator;
import io.github.pastorgl.datacooker.scripting.Utils;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.util.Deque;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class ExpressionOperators {
    public static class TERNARY1 extends Operator implements Binary {
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
        public boolean rightAssoc() {
            return true;
        }

        @Override
        protected boolean handleNull() {
            return true;
        }
    }

    public static class DEFAULT extends TERNARY1 {
        @Override
        public String name() {
            return "DEFAULT";
        }
    }

    public static class TERNARY2 extends Operator implements Binary {
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
    }

    public static class OR extends Operator implements Binary {
        @Override
        public int prio() {
            return 10;
        }

        @Override
        protected Object op0(Deque<Object> args) {
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
        protected boolean handleNull() {
            return true;
        }
    }

    public static class XOR extends Operator implements Binary {
        @Override
        public int prio() {
            return 10;
        }

        @Override
        protected Object op0(Deque<Object> args) {
            boolean y = Evaluator.peekNull(args);
            boolean a = !y && Evaluator.popBoolean(args);
            boolean z = Evaluator.peekNull(args);
            boolean b = !z && Evaluator.popBoolean(args);
            return (y && z) ? null : (a != b);
        }

        @Override
        public String name() {
            return "OR";
        }

        @Override
        protected boolean handleNull() {
            return true;
        }
    }

    public static class AND extends Operator implements Binary {
        @Override
        public int prio() {
            return 20;
        }

        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = Evaluator.popBoolean(args);
            boolean b = Evaluator.popBoolean(args);
            return a && b;
        }

        @Override
        public String name() {
            return "AND";
        }
    }

    public static class NOT extends Operator implements Unary {
        @Override
        public int prio() {
            return 30;
        }

        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = Evaluator.peekNull(args);
            return a ? null : !Evaluator.popBoolean(args);
        }

        @Override
        public String name() {
            return "NOT";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        protected boolean handleNull() {
            return true;
        }
    }

    public static class EQ extends Operator implements Binary {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return "=";
        }

        @Override
        protected boolean handleNull() {
            return true;
        }

        @Override
        protected Object op0(Deque<Object> args) {
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
            return Objects.equals(a, b);
        }
    }

    public static class EQ2 extends EQ {
        @Override
        public String name() {
            return "==";
        }
    }

    public static class NEQ extends Operator implements Binary {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return "!=";
        }

        @Override
        protected boolean handleNull() {
            return true;
        }

        @Override
        protected Object op0(Deque<Object> args) {
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
            return !Objects.equals(a, b);
        }
    }

    public static class NE2 extends NEQ {
        @Override
        public String name() {
            return "<>";
        }
    }

    public static class GE extends Operator implements Binary {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return ">=";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a >= b;
        }
    }

    public static class GT extends Operator implements Binary {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return ">";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a > b;
        }
    }

    public static class LE extends Operator implements Binary {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return "<=";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a <= b;
        }
    }

    public static class LT extends Operator implements Binary {
        @Override
        public int prio() {
            return 40;
        }

        @Override
        public String name() {
            return "<";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a < b;
        }
    }

    public static class LIKE extends Operator implements Binary {
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
        protected Object op0(Deque<Object> args) {
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
        public String name() {
            return "MATCH";
        }
    }

    public static class REGEX extends LIKE {
        @Override
        public String name() {
            return "REGEX";
        }
    }

    public static class DIGEST extends Operator implements Binary {
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
        protected Object op0(Deque<Object> args) {
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
        public String name() {
            return "HASH";
        }
    }

    public static class BOR extends Operator implements Binary {
        @Override
        public int prio() {
            return 105;
        }

        @Override
        public String name() {
            return "|";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a | b;
        }
    }

    public static class BXOR extends Operator implements Binary {
        @Override
        public int prio() {
            return 110;
        }

        @Override
        public String name() {
            return "#";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a ^ b;
        }
    }

    public static class BAND extends Operator implements Binary {
        @Override
        public int prio() {
            return 115;
        }

        @Override
        public String name() {
            return "&";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a & b;
        }
    }

    public static class BSL extends Operator implements Binary {
        @Override
        public int prio() {
            return 120;
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
        protected Object op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a << b;
        }
    }

    public static class BSR extends Operator implements Binary {
        @Override
        public int prio() {
            return 120;
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
        protected Object op0(Deque<Object> args) {
            long a = Evaluator.popLong(args);
            long b = Evaluator.popLong(args);
            return a >> b;
        }
    }

    public static class CAT extends Operator implements Binary {
        @Override
        public int prio() {
            return 125;
        }

        @Override
        public String name() {
            return "||";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            return args.stream().map(String::valueOf).collect(Collectors.joining());
        }
    }

    public static class ADD extends Operator implements Binary {
        @Override
        public int prio() {
            return 125;
        }

        @Override
        public String name() {
            return "+";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a + b;
        }
    }

    public static class SUB extends Operator implements Binary {
        @Override
        public int prio() {
            return 125;
        }

        @Override
        public String name() {
            return "-";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a - b;
        }
    }

    public static class MUL extends Operator implements Binary {
        @Override
        public int prio() {
            return 130;
        }

        @Override
        public String name() {
            return "*";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a * b;
        }
    }

    public static class DIV extends Operator implements Binary {
        @Override
        public int prio() {
            return 130;
        }

        @Override
        public String name() {
            return "/";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a / b;
        }
    }

    public static class MOD extends Operator implements Binary {
        @Override
        public int prio() {
            return 130;
        }

        @Override
        public String name() {
            return "%";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return a % b;
        }
    }

    public static class EXP extends Operator implements Binary {
        @Override
        public int prio() {
            return 135;
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
        protected Object op0(Deque<Object> args) {
            double a = Evaluator.popDouble(args);
            double b = Evaluator.popDouble(args);
            return Math.pow(a, b);
        }
    }

    public static class BNOT extends Operator implements Unary {
        @Override
        public int prio() {
            return 140;
        }

        @Override
        protected Object op0(Deque<Object> args) {
            return ~Evaluator.popLong(args);
        }

        @Override
        public String name() {
            return "~";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }
}
