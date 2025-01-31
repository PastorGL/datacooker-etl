/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operators;

import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Operator;

import java.util.Deque;

@SuppressWarnings("unused")
public class BooleanOperators {
    public static class OR extends Operator.Binary<Boolean, Boolean, Boolean> {
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

    public static class XOR extends Operator.Binary<Boolean, Boolean, Boolean> {
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

    public static class AND extends Operator.Binary<Boolean, Boolean, Boolean> {
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

    public static class NOT extends Operator.Unary<Boolean, Boolean> {
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
}
