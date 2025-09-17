/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.Pluggable;
import io.github.pastorgl.datacooker.metadata.PluggableInfo;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;

public class TDLTransform {
    public static Builder builder(String name, StreamType.StreamTypes from, StreamType.StreamTypes into, List<StatementItem> items, VariablesContext vc) {
        return new Builder(name, from, into, items, vc);
    }

    public static StatementItem fetch(String[] control) {
        return new StatementItem.Builder(TDLStatement.FETCH).control(control).build();
    }

    public static StatementItem yield(List<Expressions.ExprItem<?>>[] expression) {
        return new StatementItem.Builder(TDLStatement.YIELD).expression(expression).build();
    }

    public static StatementItem transformReturn() {
        return new StatementItem.Builder(TDLStatement.RETURN).build();
    }

    public static StatementItem let(String controlVar, List<Expressions.ExprItem<?>> expression) {
        return new StatementItem.Builder(TDLStatement.LET).control(controlVar).expression(expression).build();
    }

    public static StatementItem transformIf(List<Expressions.ExprItem<?>> expression, List<StatementItem> ifBranch, List<StatementItem> elseBranch) {
        return new StatementItem.Builder(TDLStatement.IF).expression(expression).mainBranch(ifBranch).elseBranch(elseBranch).build();
    }

    public static StatementItem loop(String controlVar, List<Expressions.ExprItem<?>> expression, List<StatementItem> loopBranch, List<StatementItem> elseBranch) {
        return new StatementItem.Builder(TDLStatement.LOOP).control(controlVar).expression(expression).mainBranch(loopBranch).elseBranch(elseBranch).build();
    }

    public static StatementItem raise(String level, List<Expressions.ExprItem<?>> expression) {
        return new StatementItem.Builder(TDLStatement.RAISE).control(level).expression(expression).build();
    }

    public static class Builder extends ParamsBuilder<Builder> {
        private final String name;
        private final StringJoiner descr = new StringJoiner(", ");
        private final List<StatementItem> items;
        private final VariablesContext vc;
        private final StreamType resultType;

        private final PluggableMetaBuilder metaBuilder;

        private Builder(String name, StreamType.StreamTypes from, StreamType.StreamTypes into, List<StatementItem> items, VariablesContext vc) {
            this.name = name;
            this.items = items;
            this.vc = vc;

            this.metaBuilder = new PluggableMetaBuilder(name);
            metaBuilder.transform();
            metaBuilder.input(from, "Input DS types");
            metaBuilder.output(into, "Output DS type");
            this.resultType = into.types[0];
        }

        public Builder mandatory(String name) {
            descr.add("@" + name);
            metaBuilder.def(name, null);
            return this;
        }

        public Builder optional(String name, Object value) {
            descr.add("@" + name + " = " + value);
            metaBuilder.def(name, null, value.getClass(), value, null);
            return this;
        }

        public PluggableInfo build() {
            metaBuilder.descr(descr.toString());

            PluggableMeta meta = metaBuilder.build();

            Pluggable<?, ?> transformer = new FunctionTransformer(name, resultType, items, vc) {
                public PluggableMeta meta() {
                    return meta;
                }
            };
            return new PluggableInfo(meta, transformer);
        }
    }

    private static abstract class FunctionTransformer extends Transformer {
        private final String name;
        private final StreamType resultType;
        private final List<StatementItem> items;
        private final VariablesContext vc;

        public FunctionTransformer(String name, StreamType resultType, List<StatementItem> items, VariablesContext vc) {
            this.name = name;
            this.resultType = resultType;
            this.items = items;
            this.vc = vc;
        }

        protected StreamTransformer transformer() {
            return (ds, newColumns, params) -> {
                VariablesContext thisCall = new VariablesContext(vc);
                for (String param : params.definitions()) {
                    thisCall.put(param, params.get(param));
                }

                Broadcast<VariablesContext> broadVars = JavaSparkContext.fromSparkContext(ds.rdd().context()).broadcast(thisCall);
                Broadcast<List<StatementItem>> broadStmt = JavaSparkContext.fromSparkContext(ds.rdd().context()).broadcast(items);

                return new DataStreamBuilder(outputName, null)
                        .transformed(name, (resultType == StreamType.Passthru) ? ds.streamType : resultType, ds)
                        .build(ds.rdd().mapPartitionsToPair(it -> {
                            List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                            VariablesContext vars = new VariablesContext(broadVars.getValue());
                            List<StatementItem> stmts = broadStmt.getValue();

                            CallContext cc = new CallContext(it, ret);
                            while (!cc.returnReached) {
                                cc.eval(stmts, vars);
                            }

                            return ret.iterator();
                        }, true));
            };
        }
    }

    private static class CallContext {
        private final Iterator<Tuple2<Object, DataRecord<?>>> it;
        private final List<Tuple2<Object, DataRecord<?>>> ret;

        private Object key;
        private DataRecord<?> rec;

        boolean returnReached = false;

        public CallContext(Iterator<Tuple2<Object, DataRecord<?>>> it, List<Tuple2<Object, DataRecord<?>>> ret) {
            this.it = it;
            this.ret = ret;
        }

        void eval(List<StatementItem> items, VariablesContext vc) {
            for (StatementItem fi : items) {
                if (returnReached) {
                    return;
                }

                switch (fi.statement) {
                    case FETCH: {
                        if (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> t = it.next();

                            key = t._1;
                            rec = t._2;

                            if (fi.control.length == 1) {
                                vc.put(fi.control[0], rec);
                            } else if (fi.control.length == 2) {
                                vc.put(fi.control[0], key);
                                vc.put(fi.control[1], rec);
                            }

                            vc.put(Constants.FETCH_VAR, false);
                        } else {
                            key = null;
                            rec = null;

                            vc.put(Constants.FETCH_VAR, true);
                        }
                        break;
                    }
                    case YIELD: {
                        Object rec = Expressions.eval(key, this.rec, fi.expression[1], vc);
                        ret.add(new Tuple2<>(Expressions.eval(key, this.rec, fi.expression[0], vc),
                                (rec instanceof DataRecord<?>) ? (DataRecord<?>) rec : new PlainText(String.valueOf(rec))));
                        break;
                    }
                    case RETURN: {
                        returnReached = true;
                        return;
                    }
                    case LET: {
                        vc.put(fi.control[0], Expressions.eval(key, rec, fi.expression[0], vc));
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
                            loopValues = new ArrayWrap(expr).data();

                            loop = loopValues.length > 0;
                        }

                        if (loop) {
                            VariablesContext vvc = new VariablesContext(vc);
                            for (Object loopValue : loopValues) {
                                if (returnReached) {
                                    return;
                                }

                                vvc.put(fi.control[0], loopValue);
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
                        break;
                    }
                }
            }
        }
    }
}
