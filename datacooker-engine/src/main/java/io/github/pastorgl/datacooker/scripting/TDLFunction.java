/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.metadata.FunctionInfo;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.Deque;
import java.util.List;
import java.util.StringJoiner;

import static io.github.pastorgl.datacooker.scripting.ProceduralStatement.*;

public class TDLFunction {
    public static Builder builder(String name, String descr, List<StatementItem> items, VariablesContext vc) {
        return new Builder(name, descr, items, vc);
    }

    public static StatementItem funcReturn(List<Expressions.ExprItem<?>> expression) {
        return new StatementItem.Builder(RETURN).expression(expression).build();
    }

    public static StatementItem let(String controlVar, List<Expressions.ExprItem<?>> expression) {
        return new StatementItem.Builder(LET).control(controlVar).expression(expression).build();
    }

    public static StatementItem funcIf(List<Expressions.ExprItem<?>> expression, List<StatementItem> ifBranch, List<StatementItem> elseBranch) {
        return new StatementItem.Builder(IF).expression(expression).mainBranch(ifBranch).elseBranch(elseBranch).build();
    }

    public static StatementItem loop(String controlVar, List<Expressions.ExprItem<?>> expression, List<StatementItem> loopBranch, List<StatementItem> elseBranch) {
        return new StatementItem.Builder(LOOP).control(controlVar).expression(expression).mainBranch(loopBranch).elseBranch(elseBranch).build();
    }

    public static StatementItem raise(String level, List<Expressions.ExprItem<?>> expression) {
        return new StatementItem.Builder(RAISE).control(level).expression(expression).build();
    }

    public static class Builder extends ParamsBuilder<Builder> {
        private final String name;
        private final String descr;
        private final StringJoiner descrJoiner = new StringJoiner(", ");
        private final List<StatementItem> items;
        private final VariablesContext vc;

        private Builder(String name, String descr, List<StatementItem> items, VariablesContext vc) {
            this.name = name;
            this.descr = descr;
            this.items = items;
            this.vc = vc;
        }

        public Builder mandatory(String name, String comment) {
            if (this.descr == null) {
                descrJoiner.add("@" + name);
            }
            return super.mandatory(name, comment);
        }

        public Builder optional(String name, String comment, Object value, String defComment) {
            if (this.descr == null) {
                descrJoiner.add("@" + name + " = " + value);
            }
            return super.optional(name, comment, value, defComment);
        }

        public FunctionInfo loose() {
            return new FunctionInfo(new LooseFunction(name, (descr == null) ? descrJoiner.toString() : descr, params, items, vc));
        }

        public FunctionInfo recordLevel(String[] recVars) {
            return new FunctionInfo(new RecordFunction(name, (descr == null) ? descrJoiner.toString() : descr, recVars, params, items, vc));
        }
    }

    private static class LooseFunction extends Function.ArbitrAry<Object, Object> {
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
                thisCall.putHere(params.get(i), (a == null) ? params.getValue(i).defaults : a);
            }

            CallContext cc = new CallContext(null, null);
            cc.eval(items, thisCall);
            if (cc.returnReached) {
                return cc.returnValue;
            }
            throw new RuntimeException("Called function " + name + " with no RETURN");
        }
    }

    private static class RecordFunction extends Function.WholeRecord<Object, DataRecord<?>> {
        protected final String name;
        protected final String descr;
        private final String[] recVars;
        protected final ListOrderedMap<String, Param> params;
        protected final List<StatementItem> items;
        protected final VariablesContext vc;

        public RecordFunction(String name, String descr, String[] recVars, ListOrderedMap<String, Param> params,
                              List<StatementItem> items, VariablesContext vc) {
            this.name = name;
            this.descr = descr;
            this.recVars = recVars;
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
                thisCall.putHere(params.get(i), (a == null) ? params.getValue(i).defaults : a);
            }

            if (recVars != null) {
                if (recVars.length == 1) {
                    thisCall.putHere(recVars[0], rec);
                } else if (recVars.length == 2) {
                    thisCall.putHere(recVars[0], key);
                    thisCall.putHere(recVars[1], rec);
                }
            }

            CallContext cc = new CallContext(key, rec);
            cc.eval(items, thisCall);
            if (cc.returnReached) {
                return cc.returnValue;
            }
            throw new RuntimeException("Called function " + name + " with no RETURN");
        }
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
                        returnValue = Expressions.eval(key, rec, fi.expression[0], vc);
                        returnReached = true;
                        return;
                    }
                    case LET: {
                        Object o = Expressions.eval(key, rec, fi.expression[0], vc);
                        if (fi.control[0] != null) {
                            vc.put(fi.control[0], o);
                        }
                        break;
                    }
                    case IF: {
                        if (Expressions.bool(key, rec, fi.expression[0], vc)) {
                            eval(fi.mainBranch, vc);
                        } else {
                            if (fi.elseBranch != null) {
                                eval(fi.elseBranch, vc);
                            }
                        }
                        break;
                    }
                    case LOOP: {
                        Object expr = Expressions.eval(key, rec, fi.expression[0], vc);
                        boolean loop = expr != null;

                        Object[] loopValues = null;
                        if (loop) {
                            loopValues = new ArrayWrap(expr).toArray();

                            loop = loopValues.length > 0;
                        }

                        if (loop) {
                            VariablesContext vvc = new VariablesContext(vc);
                            for (Object loopValue : loopValues) {
                                if (returnReached) {
                                    return;
                                }

                                vvc.putHere(fi.control[0], loopValue);
                                eval(fi.mainBranch, vvc);
                            }
                        } else {
                            if (fi.elseBranch != null) {
                                eval(fi.elseBranch, vc);
                            }
                        }
                        break;
                    }
                    case RAISE: {
                        Object msg = Expressions.eval(key, rec, fi.expression[0], vc);

                        switch (MsgLvl.get(fi.control[0])) {
                            case INFO -> System.out.println(msg);
                            case WARNING -> System.err.println(msg);
                            default -> {
                                returnReached = true;
                                throw new RaiseException(String.valueOf(msg));
                            }
                        }
                    }
                }
            }
        }
    }
}