/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataRecord;

public abstract class Function<R> implements Evaluator<R> {
    public static final int WHOLE_RECORD = -4;
    public static final int RECORD_OBJECT = -3;
    public static final int RECORD_KEY = -2;
    public static final int ARBITR_ARY = -1;
    public static final int NO_ARGS = 0;

    public Function() {
    }

    public static abstract class RecordKey<R> extends Function<R> {
        @Override
        public int arity() {
            return RECORD_KEY;
        }
    }

    public static abstract class RecordObject<R, REC extends DataRecord<?>> extends Function<R> {
        @Override
        public int arity() {
            return RECORD_OBJECT;
        }
    }

    public static abstract class WholeRecord<R> extends Function<R> {
        @Override
        public int arity() {
            return WHOLE_RECORD;
        }
    }

    public static abstract class ArbitrAry<R, TA> extends Function<R> {
        @Override
        public int arity() {
            return ARBITR_ARY;
        }
    }

    public static abstract class NoArgs<R> extends Function<R> {
        @Override
        public int arity() {
            return NO_ARGS;
        }
    }

    public static abstract class Unary<R, T> extends Function<R> {
        @Override
        public int arity() {
            return 1;
        }
    }

    public static abstract class Binary<R, T1, T2> extends Function<R> {
        @Override
        public int arity() {
            return 2;
        }
    }

    public static abstract class Ternary<R, T1, T2, T3> extends Function<R> {
        @Override
        public int arity() {
            return 3;
        }
    }
}
