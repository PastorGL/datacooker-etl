/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions;

import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function.ArbitrAry;
import io.github.pastorgl.datacooker.scripting.Function.Binary;
import io.github.pastorgl.datacooker.scripting.Function.NoArgs;
import io.github.pastorgl.datacooker.scripting.Function.Unary;
import io.github.pastorgl.datacooker.scripting.Utils;

import java.util.Deque;

@SuppressWarnings("unused")
public class NumericFunctions {
    public static class CEIL extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.ceil(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "CEIL";
        }

        @Override
        public String descr() {
            return "Returns the smallest Double value that is greater than or equal to the" +
                    " argument and is equal to a mathematical integer";
        }
    }

    public static class FLOOR extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.floor(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "FLOOR";
        }

        @Override
        public String descr() {
            return "Returns the largest Double value that is less than or equal to the" +
                    " argument and is equal to a mathematical integer";
        }
    }

    public static class ROUND extends Unary<Long, Double> {
        @Override
        public Long call(Deque<Object> args) {
            return Math.round(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ROUND";
        }

        @Override
        public String descr() {
            return "Returns the closest Long to the argument, with ties rounding to positive infinity";
        }
    }

    public static class LogAnyBase extends Binary<Double, Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            double base = Evaluator.popDouble(args);
            double a = Evaluator.popDouble(args);
            return Math.log(a) / Math.log(base);
        }

        @Override
        public String name() {
            return "LOG";
        }

        @Override
        public String descr() {
            return "Returns the logarithm with base from 1st argument of a Double" +
                    " value form 2nd";
        }
    }

    public static class LogNatural extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.log(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "LN";
        }

        @Override
        public String descr() {
            return "Returns the natural logarithm of a given Double value";
        }
    }

    public static class Log10 extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.log10(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "LG";
        }

        @Override
        public String descr() {
            return "Returns the base 10 logarithm of a given Double value";
        }
    }

    public static class SIN extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.sin(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "SIN";
        }

        @Override
        public String descr() {
            return "Returns the trigonometric sine of an angle (in radians)";
        }
    }

    public static class COS extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.cos(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "COS";
        }

        @Override
        public String descr() {
            return "Returns the trigonometric cosine of an angle (in radians)";
        }
    }

    public static class TAN extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.tan(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "TAN";
        }

        @Override
        public String descr() {
            return "Returns the trigonometric tangent of an angle (in radians)";
        }
    }

    public static class SINH extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.sinh(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "SINH";
        }

        @Override
        public String descr() {
            return "Returns the hyperbolic sine of a Double";
        }
    }

    public static class COSH extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.cosh(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "COSH";
        }

        @Override
        public String descr() {
            return "Returns the hyperbolic cosine of a Double";
        }
    }

    public static class TANH extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.tanh(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "TANH";
        }

        @Override
        public String descr() {
            return "Returns the hyperbolic tangent of a Double";
        }
    }

    public static class ASIN extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.asin(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ASIN";
        }

        @Override
        public String descr() {
            return "Returns the arc sine of a value; the returned angle is in the -PI/2 through PI/2";
        }
    }

    public static class ACOS extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.acos(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ACOS";
        }

        @Override
        public String descr() {
            return "Returns the arc sine of a value; the returned angle is in the 0 through PI";
        }
    }

    public static class ATAN extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.atan(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ATAN";
        }

        @Override
        public String descr() {
            return "Returns the arc tangent of a value; the returned angle is in the range -PI/2 through PI/2";
        }
    }

    public static class ATAN2 extends Binary<Double, Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.atan2(Evaluator.popDouble(args), Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ATAN2";
        }

        @Override
        public String descr() {
            return "Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar" +
                    " coordinates (r, theta)";
        }
    }

    public static class E extends NoArgs<Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.E;
        }

        @Override
        public String name() {
            return "E";
        }

        @Override
        public String descr() {
            return "Base of the natural logarithms";
        }
    }

    public static class PI extends NoArgs<Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.PI;
        }

        @Override
        public String name() {
            return "PI";
        }

        @Override
        public String descr() {
            return "Ratio of the circumference of a circle to its diameter";
        }
    }

    public static class RAD extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.toRadians(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "RAD";
        }

        @Override
        public String descr() {
            return "Converts an angle from degrees to radians";
        }
    }

    public static class DEG extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.toDegrees(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "DEG";
        }

        @Override
        public String descr() {
            return "Converts an angle from radians to degrees";
        }
    }

    public static class SIGN extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.signum(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "SIGN";
        }

        @Override
        public String descr() {
            return "1 if argument is positive, -1 if negative, 0 if zero";
        }
    }

    public static class ABS extends Unary<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            return Math.abs(Evaluator.popDouble(args));
        }

        @Override
        public String name() {
            return "ABS";
        }

        @Override
        public String descr() {
            return "Absolute value of Double";
        }
    }

    public static class MAX extends ArbitrAry<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            if (args.isEmpty()) {
                return null;
            }

            return args.stream().mapToDouble(o -> Utils.parseNumber(String.valueOf(o)).doubleValue()).max().getAsDouble();
        }

        @Override
        public String name() {
            return "MAX";
        }

        @Override
        public String descr() {
            return "Returns a mathematical max value from given arguments";
        }
    }

    public static class MIN extends ArbitrAry<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            if (args.isEmpty()) {
                return null;
            }

            return args.stream().mapToDouble(o -> Utils.parseNumber(String.valueOf(o)).doubleValue()).min().getAsDouble();
        }

        @Override
        public String name() {
            return "MIN";
        }

        @Override
        public String descr() {
            return "Returns a mathematical min value from given arguments";
        }
    }

    public static class MEDIAN extends ArbitrAry<Double, Double> {
        @Override
        public Double call(Deque<Object> args) {
            if (args.isEmpty()) {
                return null;
            }

            double[] sorted = args.stream().mapToDouble(o -> Utils.parseNumber(String.valueOf(o)).doubleValue()).sorted().toArray();

            int m = sorted.length >> 1;
            return (sorted.length % 2 == 0)
                    ? (sorted[m] + sorted[m - 1]) / 2.D
                    : sorted[m];
        }

        @Override
        public String name() {
            return "MEDIAN";
        }

        @Override
        public String descr() {
            return "Returns a mathematical median value from given arguments";
        }
    }
}
