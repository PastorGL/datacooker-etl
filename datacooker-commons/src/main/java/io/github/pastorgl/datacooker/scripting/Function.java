/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.DataRecord;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.Deque;
import java.util.List;
import java.util.StringJoiner;

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

    public static Builder builder(String name, List<Expressions.ExprItem<?>> items, VariablesContext vc) {
        return new Builder(name, items, vc);
    }

    public static class Builder {
        private final String name;
        private final StringJoiner descr = new StringJoiner(", ");
        private final List<Expressions.ExprItem<?>> items;
        private final ListOrderedMap<String, Param> params = new ListOrderedMap<>();
        private final VariablesContext vc;

        private Builder(String name, List<Expressions.ExprItem<?>> items, VariablesContext vc) {
            this.name = name;
            this.items = items;
            this.vc = new VariablesContext(vc);
        }

        public Builder mandatory(String name) {
            params.put(name, new Param());
            descr.add("@" + name);
            return this;
        }

        public Builder optional(String name, Object value) {
            params.put(name, new Param(value));
            descr.add("@" + name + " = " + value);
            return this;
        }

        public Function<Object> buildLoose() {
            return new LooseFunction(name, descr.toString(), params, items, vc);
        }

        public Function<Object> buildRecord() {
            return new RecordFunction(name, descr.toString(), params, items, vc);
        }
    }

    private static class LooseFunction extends ArbitrAry<Object, Object> {
        private final String name;
        private final String descr;
        private final ListOrderedMap<String, Param> params;
        private final List<Expressions.ExprItem<?>> items;
        private final VariablesContext vc;

        private LooseFunction(String name, String descr, ListOrderedMap<String, Param> params,
                              List<Expressions.ExprItem<?>> items, VariablesContext vc) {
            this.name = name;
            this.descr = descr;
            this.params = params;
            this.items = items;
            this.vc = vc;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String descr() {
            return descr;
        }

        @Override
        public Object call(Deque<Object> args) {
            for (int i = 0; i < params.size(); i++) {
                Object a = args.pop();
                vc.put(params.get(i), (a == null) ? params.getValue(i).defaults : a);
            }
            return Expressions.evalLoose(items, vc);
        }
    }

    private static class RecordFunction extends WholeRecord<Object, DataRecord<?>> {
        private final String name;
        private final String descr;
        private final ListOrderedMap<String, Param> params;
        private final List<Expressions.ExprItem<?>> items;
        private final VariablesContext vc;

        public RecordFunction(String name, String descr, ListOrderedMap<String, Param> params,
                              List<Expressions.ExprItem<?>> items, VariablesContext vc) {
            this.name = name;
            this.descr = descr;
            this.params = params;
            this.items = items;
            this.vc = vc;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String descr() {
            return descr;
        }

        @Override
        public Object call(Deque<Object> args) {
            Object key = args.pop();
            Object rec = args.pop();
            for (int i = 0; i < params.size(); i++) {
                Object a = args.pop();
                vc.put(params.get(i), (a == null) ? params.getValue(i).defaults : a);
            }
            return Expressions.evalAttr(key, (DataRecord<?>) rec, items, vc);
        }
    }
}
