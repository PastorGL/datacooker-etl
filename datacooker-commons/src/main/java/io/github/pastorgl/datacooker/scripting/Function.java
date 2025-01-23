/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataRecord;

import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Function<R> implements Evaluator<R> {
    // record-related arities, allowed only in query context
    public static final int RECORD_LEVEL = -5;
    public static final int WHOLE_RECORD = -4;
    public static final int RECORD_OBJECT = -3;
    public static final int RECORD_KEY = -2;
    // anything above 'arbitrary' is allowed everywhere
    public static final int ARBITR_ARY = -1;
    public static final int NO_ARGS = 0;

    public Function() {
    }

    public static abstract class RecordLevel<R> extends Function<R> {
        @Override
        public int arity() {
            return RECORD_LEVEL;
        }
    }

    public static abstract class WholeRecord<R, REC extends DataRecord<?>> extends Function<R> {
        @Override
        public int arity() {
            return WHOLE_RECORD;
        }
    }

    public static abstract class RecordObject<R, REC extends DataRecord<?>> extends Function<R> {
        @Override
        public int arity() {
            return RECORD_OBJECT;
        }
    }

    public static abstract class RecordKey<R> extends Function<R> {
        @Override
        public int arity() {
            return RECORD_KEY;
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

    public static Builder builder(List<Expressions.ExprItem<?>> items) {
        return new Builder(items);
    }

    public static class Builder {
        private final List<Expressions.ExprItem<?>> items;
        private final Map<String, Param> params = new HashMap<>();

        private Builder(List<Expressions.ExprItem<?>> items) {
            this.items = items;
        }

        public Builder mandatory(String name) {
            params.put(name, new Param());
            return this;
        }

        public Builder optional(String name, Object value) {
            params.put(name, new Param(value));
            return this;
        }

        public Function<Object> buildLoose() {
            return new ArbitrAry<Object, Object>() {
                @Override
                public String name() {
                    return "";
                }

                @Override
                public String descr() {
                    return "";
                }

                @Override
                public Object call(Deque args) {
                    return null;
                }
            };
        }

        public Function<Object> buildRecord() {
            return new RecordLevel<Object>() {
                @Override
                public Object call(Deque<Object> args) {
                    return null;
                }

                @Override
                public String name() {
                    return "";
                }

                @Override
                public String descr() {
                    return "";
                }
            };
        }
    }
}
