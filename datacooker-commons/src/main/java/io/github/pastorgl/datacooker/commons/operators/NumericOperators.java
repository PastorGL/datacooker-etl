/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operators;

import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Operator;

import java.util.Deque;
import java.util.Random;

@SuppressWarnings("unused")
public class NumericOperators {
    public static class GE extends Operator.Binary<Boolean, Double, Double> {
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

    public static class GT extends Operator.Binary<Boolean, Double, Double> {
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

    public static class LE extends Operator.Binary<Boolean, Double, Double> {
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

    public static class LT extends Operator.Binary<Boolean, Double, Double> {
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

    public static class BOR extends Operator.Binary<Long, Long, Long> {
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

    public static class BXOR extends Operator.Binary<Long, Long, Long> {
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

    public static class BAND extends Operator.Binary<Long, Long, Long> {
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

    public static class BSL extends Operator.Binary<Long, Long, Long> {
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

    public static class BSR extends Operator.Binary<Long, Long, Long> {
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

    public static class ADD extends Operator.Binary<Number, Number, Number> {
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
        protected Number op0(Deque<Object> args) {
            Number a = Evaluator.popNumeric(args);
            Number b = Evaluator.popNumeric(args);
            if (a instanceof Double || b instanceof Double) {
                return a.doubleValue() + b.doubleValue();
            }
            if (a instanceof Long || b instanceof Long) {
                return a.longValue() + b.longValue();
            }
            return a.intValue() + b.intValue();
        }
    }

    public static class SUB extends Operator.Binary<Number, Number, Number> {
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
        protected Number op0(Deque<Object> args) {
            Number a = Evaluator.popNumeric(args);
            Number b = Evaluator.popNumeric(args);
            if (a instanceof Double || b instanceof Double) {
                return a.doubleValue() - b.doubleValue();
            }
            if (a instanceof Long || b instanceof Long) {
                return a.longValue() - b.longValue();
            }
            return a.intValue() - b.intValue();
        }
    }

    public static class MUL extends Operator.Binary<Number, Number, Number> {
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
        protected Number op0(Deque<Object> args) {
            Number a = Evaluator.popNumeric(args);
            Number b = Evaluator.popNumeric(args);
            if (a instanceof Double || b instanceof Double) {
                return a.doubleValue() * b.doubleValue();
            }
            if (a instanceof Long || b instanceof Long) {
                return a.longValue() * b.longValue();
            }
            return a.intValue() * b.intValue();
        }
    }

    public static class DIV extends Operator.Binary<Double, Number, Number> {
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

    public static class MOD extends Operator.Binary<Double, Double, Double> {
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

    public static class EXP extends Operator.Binary<Double, Double, Double> {
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

    public static class BNOT extends Operator.Unary<Long, Long> {
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

    public static class RANDOM extends Operator.Unary<Integer, Integer> {
        @Override
        public int prio() {
            return 145;
        }

        @Override
        public boolean onlyNumerics() {
            return true;
        }

        @Override
        public Integer op0(Deque<Object> args) {
            int a = Evaluator.popInt(args);
            if (a == 0) {
                return 0;
            }
            return (a < 0)
                    ? -new Random().nextInt(-a)
                    : new Random().nextInt(a);
        }

        @Override
        public String name() {
            return "RANDOM";
        }

        @Override
        public String descr() {
            return "Returns a pseudorandom, uniformly distributed Integer value" +
                    " between 0 (inclusive) and the specified value (exclusive)";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class RAND extends RANDOM {
        @Override
        public String descr() {
            return "Alias of RANDOM";
        }

        @Override
        public String name() {
            return "RAND";
        }
    }
}
