/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.functions;

import io.github.pastorgl.datacooker.scripting.Function;
import io.github.pastorgl.datacooker.scripting.Evaluator.Binary;
import io.github.pastorgl.datacooker.scripting.Evaluator.Unary;
import io.github.pastorgl.datacooker.scripting.Evaluator;

import java.util.Deque;
import java.util.Random;

@SuppressWarnings("unused")
public class NumericFunctions {
    public static class CEIL extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.ceil(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "CEIL";
        }
    }

    public static class FLOOR extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.floor(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "FLOOR";
        }
    }

    public static class ROUND extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.round(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ROUND";
        }
    }

    public static class LogAnyBase extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            double base = Evaluator.popDouble(args);
            double a = Evaluator.popDouble(args);
            return Math.log(a) / Math.log(base);
        }

        @Override
        public String name() {
            return "LOG";
        }
    }

    public static class LogNatural extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.log(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "LN";
        }
    }

    public static class Log10 extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.log10(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "LG";
        }
    }

    public static class SIN extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.sin(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "SIN";
        }
    }

    public static class COS extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.cos(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "COS";
        }
    }

    public static class TAN extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.tan(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "TAN";
        }
    }

    public static class SINH extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.sinh(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "SINH";
        }
    }

    public static class COSH extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.cosh(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "COSH";
        }
    }

    public static class TANH extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.tanh(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "TANH";
        }
    }

    public static class ASIN extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.asin(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ASIN";
        }
    }

    public static class ACOS extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.acos(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ACOS";
        }
    }

    public static class ATAN extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.atan(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ATAN";
        }
    }

    public static class ATAN2 extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.atan2(Evaluator.popDouble(args), Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ATAN2";
        }
    }

    public static class EXP extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            double base = Evaluator.popDouble(args);
            return (base == 1.D) ? Math.E : Math.exp(base);
        }

        @Override
        public String name() {
            return "EXP";
        }
    }

    public static class PI extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            double base = Evaluator.popDouble(args);
            return (base == 1.D) ? Math.PI : Math.pow(Math.PI, base);
        }

        @Override
        public String name() {
            return "PI";
        }
    }

    public static class RAD extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.toRadians(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "RAD";
        }
    }

    public static class DEG extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.toDegrees(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "DEG";
        }
    }

    public static class SIGN extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.signum(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "SIGN";
        }
    }

    public static class ABS extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.abs(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ABS";
        }
    }

    public static class RAND extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
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
            return "RAND";
        }
    }
}
