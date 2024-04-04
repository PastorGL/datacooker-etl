/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.functions;

import io.github.pastorgl.datacooker.scripting.Function.Binary;
import io.github.pastorgl.datacooker.scripting.Function.Unary;
import io.github.pastorgl.datacooker.scripting.Operator;

import java.util.Deque;

@SuppressWarnings("unused")
public class MathFunctions {
    public static class CEIL extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.ceil(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "CEIL";
        }
    }

    public static class FLOOR extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.floor(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "FLOOR";
        }
    }

    public static class ROUND extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.round(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "ROUND";
        }
    }

    public static class LogAnyBase extends Binary {
        @Override
        public Object call(Deque<Object> args) {
            double base = Operator.popDouble(args);
            double a = Operator.popDouble(args);
            return Math.log(a) / Math.log(base);
        }

        @Override
        public String name() {
            return "LOG";
        }
    }

    public static class LogNatural extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.log(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "LN";
        }
    }

    public static class Log10 extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.log10(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "LG";
        }
    }

    public static class SIN extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.sin(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "SIN";
        }
    }

    public static class COS extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.cos(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "COS";
        }
    }

    public static class TAN extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.tan(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "TAN";
        }
    }

    public static class SINH extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.sinh(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "SINH";
        }
    }

    public static class COSH extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.cosh(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "COSH";
        }
    }

    public static class TANH extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.tanh(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "TANH";
        }
    }

    public static class ASIN extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.asin(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "ASIN";
        }
    }

    public static class ACOS extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.acos(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "ACOS";
        }
    }

    public static class ATAN extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.atan(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "ATAN";
        }
    }

    public static class ATAN2 extends Binary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.atan2(Operator.popDouble(args), Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "ATAN2";
        }
    }

    public static class EXP extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            double base = Operator.popDouble(args);
            return (base == 1.D) ? Math.E : Math.exp(base);
        }

        @Override
        public String name() {
            return "EXP";
        }
    }

    public static class PI extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            double base = Operator.popDouble(args);
            return (base == 1.D) ? Math.PI : Math.pow(Math.PI, base);
        }

        @Override
        public String name() {
            return "PI";
        }
    }

    public static class RAD extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.toRadians(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "RAD";
        }
    }

    public static class DEG extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.toDegrees(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "DEG";
        }
    }

    public static class SIGN extends Unary {
        @Override
        public Object call(Deque<Object> args) {
            return Math.signum(Operator.popDouble(args));
        }

        @Override
        public String name() {
            return "SIGN";
        }
    }
}
