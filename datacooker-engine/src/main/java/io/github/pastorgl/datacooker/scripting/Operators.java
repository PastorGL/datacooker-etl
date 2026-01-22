/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.metadata.OperatorInfo;
import io.github.pastorgl.datacooker.scripting.Operator.Binary;
import io.github.pastorgl.datacooker.scripting.Operator.Ternary;

import java.util.*;

public class Operators {
    // Those three are in fact peculiar language constructs, so we make sure they
    // won't go into the list of real Expression Operators. Therefore, this class
    // is not included in @RegisteredPackage, and we directly reference them in
    // the interpreter instead.
    public static OperatorInfo IN = new OperatorInfo(new IN());
    public static OperatorInfo IS = new OperatorInfo(new IS());
    public static OperatorInfo BETWEEN = new OperatorInfo(new BETWEEN());

    public static class IN extends Binary<Boolean, Object, Object> {
        @Override
        public int prio() {
            return 35;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public boolean handleNull() {
            return true;
        }

        @Override
        public String name() {
            return "IN";
        }

        @Override
        public String descr() {
            return "TRUE if left argument is present in the right, casted to array, FALSE otherwise." +
                    " Vice versa for NOT variant";
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            Object n = args.pop();

            if (Evaluator.peekNull(args)) {
                return false;
            }

            ArrayWrap h = Evaluator.popArray(args);
            if (h.size() == 0) {
                return false;
            }

            Collection<?> haystack = Arrays.asList(h.data());
            return haystack.contains(n);
        }
    }

    public static class IS extends Binary<Boolean, Object, Void> {
        @Override
        public int prio() {
            return 35;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "IS";
        }

        @Override
        public String descr() {
            return "TRUE if left argument is NULL, FALSE otherwise. Vice versa for NOT variant";
        }

        @Override
        public boolean handleNull() {
            return true;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            return Evaluator.peekNull(args);
        }
    }

    public static class BETWEEN extends Ternary<Boolean, Double, Double, Double> {
        @Override
        public int prio() {
            return 35;
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
            return "BETWEEN";
        }

        @Override
        public String descr() {
            return "TRUE if left argument is inclusively between min and max, casted to numerics, FALSE otherwise." +
                    " For NOT variant, TRUE if it is outside the range, excluding boundaries";
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            double b = Evaluator.popDouble(args);
            double l = Evaluator.popDouble(args);
            double r = Evaluator.popDouble(args);

            return (b >= l) && (b <= r);
        }
    }
}
