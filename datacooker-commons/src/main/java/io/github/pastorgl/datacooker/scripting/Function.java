/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.DataRecord;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.io.Serializable;
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

    public static Builder builder(String name, List<StatementItem> items, VariablesContext vc) {
        return new Builder(name, items, vc);
    }

    public static class Builder {
        private final String name;
        private final StringJoiner descr = new StringJoiner(", ");
        private final List<StatementItem> items;
        private final ListOrderedMap<String, Param> params = new ListOrderedMap<>();
        private final VariablesContext vc;

        private Builder(String name, List<StatementItem> items, VariablesContext vc) {
            this.name = name;
            this.items = items;
            this.vc = vc;
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

        public ArbitrAry<Object, Object> loose() {
            return new LooseFunction(name, descr.toString(), params, items, vc);
        }

        public WholeRecord<Object, DataRecord<?>> recordLevel() {
            return new RecordFunction(name, descr.toString(), params, items, vc);
        }
    }

    private static class LooseFunction extends ArbitrAry<Object, Object> {
        protected final String name;
        protected final String descr;
        protected final ListOrderedMap<String, Param> params;
        protected final List<StatementItem> items;
        protected final VariablesContext vc;

        private LooseFunction(String name, String descr, ListOrderedMap<String, Param> params,
                              List<StatementItem> items, VariablesContext vc) {
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
            VariablesContext thisCall = new VariablesContext(vc);
            for (int i = 0; i < params.size(); i++) {
                Object a = args.pop();
                thisCall.put(params.get(i), (a == null) ? params.getValue(i).defaults : a);
            }

            CallContext cc = new CallContext(null, null);
            cc.eval(items, thisCall);
            if (cc.returnReached) {
                return cc.returnValue;
            }
            throw new RuntimeException("Called function " + name + " with no RETURN");
        }
    }

    private static class RecordFunction extends WholeRecord<Object, DataRecord<?>> {
        protected final String name;
        protected final String descr;
        protected final ListOrderedMap<String, Param> params;
        protected final List<StatementItem> items;
        protected final VariablesContext vc;

        public RecordFunction(String name, String descr, ListOrderedMap<String, Param> params,
                              List<StatementItem> items, VariablesContext vc) {
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
            VariablesContext thisCall = new VariablesContext(vc);
            Object key = args.pop();
            DataRecord<?> rec = (DataRecord<?>) args.pop();
            for (int i = 0; i < params.size(); i++) {
                Object a = args.pop();
                thisCall.put(params.get(i), (a == null) ? params.getValue(i).defaults : a);
            }

            CallContext cc = new CallContext(key, rec);
            cc.eval(items, thisCall);
            if (cc.returnReached) {
                return cc.returnValue;
            }
            throw new RuntimeException("Called function " + name + " with no RETURN");
        }
    }

    private enum Statement {
        LET, IF, LOOP, RETURN
    }

    public static class StatementItem implements Serializable {
        final Statement statement;
        final String controlVar;
        final List<Expressions.ExprItem<?>> expression;
        final List<StatementItem> mainBranch;
        final List<StatementItem> elseBranch;

        private StatementItem(Statement statement, String controlVar, List<Expressions.ExprItem<?>> expression, List<StatementItem> mainBranch, List<StatementItem> elseBranch) {
            this.statement = statement;
            this.controlVar = controlVar;
            this.expression = expression;
            this.mainBranch = mainBranch;
            this.elseBranch = elseBranch;
        }

        @Override
        public String toString() {
            return statement.name() + ((controlVar != null) ? " $" + controlVar : "");
        }
    }

    public static StatementItem funcLet(String controlVar, List<Expressions.ExprItem<?>> expression) {
        return new StatementItem(Statement.LET, controlVar, expression, null, null);
    }

    public static StatementItem funcIf(List<Expressions.ExprItem<?>> expression, List<StatementItem> ifBranch, List<StatementItem> elseBranch) {
        return new StatementItem(Statement.IF, null, expression, ifBranch, elseBranch);
    }

    public static StatementItem funcLoop(String controlVar, List<Expressions.ExprItem<?>> expression, List<StatementItem> loopBranch, List<StatementItem> elseBranch) {
        return new StatementItem(Statement.LOOP, controlVar, expression, loopBranch, elseBranch);
    }

    public static StatementItem funcReturn(List<Expressions.ExprItem<?>> expression) {
        return new StatementItem(Statement.RETURN, null, expression, null, null);
    }

    private static class CallContext {
        private final Object key;
        private final DataRecord<?> rec;

        boolean returnReached = false;
        Object returnValue = null;

        public CallContext(Object key, DataRecord<?> rec) {
            this.key = key;
            this.rec = rec;
        }

        void eval(List<StatementItem> items, VariablesContext vc) {
            for (StatementItem fi : items) {
                if (returnReached) {
                    return;
                }

                switch (fi.statement) {
                    case RETURN: {
                        returnValue = Expressions.eval(key, rec, fi.expression, vc);
                        returnReached = true;
                        return;
                    }
                    case LET: {
                        vc.put(fi.controlVar, Expressions.eval(key, rec, fi.expression, vc));
                        break;
                    }
                    case IF: {
                        if (Expressions.bool(key, rec, fi.expression, vc)) {
                            eval(fi.mainBranch, vc);
                        } else {
                            if (fi.elseBranch != null) {
                                eval(fi.elseBranch, vc);
                            }
                        }
                        break;
                    }
                    case LOOP: {
                        Object expr = Expressions.eval(key, rec, fi.expression, vc);
                        boolean loop = expr != null;

                        Object[] loopValues = null;
                        if (loop) {
                            loopValues = new ArrayWrap(expr).data();

                            loop = loopValues.length > 0;
                        }

                        if (loop) {
                            VariablesContext vvc = new VariablesContext(vc);
                            for (Object loopValue : loopValues) {
                                if (returnReached) {
                                    return;
                                }

                                vvc.put(fi.controlVar, loopValue);
                                eval(fi.mainBranch, vvc);
                            }
                        } else {
                            if (fi.elseBranch != null) {
                                eval(fi.elseBranch, vc);
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}
