/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.util.Deque;

public abstract class Operator<R> implements Evaluator<R> {
    public abstract int prio();

    public boolean rightAssoc() {
        return false;
    }

    public boolean handleNull() {
        return false;
    }

    protected abstract R op0(Deque<Object> args);

    public Operator() {
    }

    @Override
    public R call(Deque<Object> args) {
        if (!handleNull()) {
            for (Object a : args) {
                if (a == null) {
                    return null;
                }
            }
        }

        return op0(args);
    }

    public static abstract class Unary<R, T> extends Operator<R> {
        @Override
        public int arity() {
            return 1;
        }
    }

    public static abstract class Binary<R, T1, T2> extends Operator<R> {
        @Override
        public int arity() {
            return 2;
        }
    }

    public static abstract class Ternary<R, T1, T2, T3> extends Operator<R> {
        @Override
        public int arity() {
            return 3;
        }
    }
}
