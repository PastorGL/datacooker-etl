/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

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

import java.util.*;

import static io.github.pastorgl.datacooker.Constants.*;
import static io.github.pastorgl.datacooker.scripting.ProceduralStatement.*;

public class TDLTransform {
    public static Builder builder(String name, String descr, StreamType.StreamTypes from, StreamType.StreamTypes into, Map<ObjLvl, List<String>> metaAttrs, List<StatementItem> items, VariablesContext vc) {
        return new Builder(name, descr, from, into, metaAttrs, items, vc);
    }

    public static StatementItem fetch(String[] control) {
        return new StatementItem.Builder(FETCH).control(control).build();
    }

    public static StatementItem yield(List<Expressions.ExprItem<?>>[] expression) {
        return new StatementItem.Builder(YIELD).expression(expression).build();
    }

    public static StatementItem transformReturn() {
        return new StatementItem.Builder(RETURN).build();
    }

    public static StatementItem let(String controlVar, List<Expressions.ExprItem<?>> expression) {
        return new StatementItem.Builder(LET).control(controlVar).expression(expression).build();
    }

    public static StatementItem transformIf(List<Expressions.ExprItem<?>> expression, List<StatementItem> ifBranch, List<StatementItem> elseBranch) {
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
        private final StreamType resultType;
        private final Map<ObjLvl, List<String>> metaAttrs;

        private final PluggableMetaBuilder metaBuilder;

        private Builder(String name, String descr, StreamType.StreamTypes from, StreamType.StreamTypes into, Map<ObjLvl, List<String>> metaAttrs, List<StatementItem> items, VariablesContext vc) {
            this.name = name;
            this.descr = descr;
            this.items = items;
            this.vc = vc;

            this.metaBuilder = new PluggableMetaBuilder(name);
            metaBuilder.transform();
            metaBuilder.input(from, "Input DS types");
            metaBuilder.output(into, "Output DS type");
            this.resultType = into.types[0];
            this.metaAttrs = metaAttrs;
        }

        public Builder mandatory(String name, String comment) {
            if (descr == null) {
                descrJoiner.add("@" + name);
            }
            metaBuilder.def(name, comment);
            return this;
        }

        public Builder optional(String name, String comment, Object value, String defComment) {
            if (descr == null) {
                descrJoiner.add("@" + name + " = " + value);
            }
            metaBuilder.def(name, comment, value.getClass(), value, defComment);
            return this;
        }

        public PluggableInfo build() {
            metaBuilder.descr((descr == null) ? descrJoiner.toString() : descr);

            PluggableMeta meta = metaBuilder.build();

            Pluggable<?, ?> transformer = new FunctionTransformer(name, resultType, items, vc, metaAttrs) {
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
        private final Map<ObjLvl, List<String>> metaAttrs;

        public FunctionTransformer(String name, StreamType resultType, List<StatementItem> items, VariablesContext vc, Map<ObjLvl, List<String>> metaAttrs) {
            this.name = name;
            this.resultType = resultType;
            this.items = items;
            this.vc = vc;
            this.metaAttrs = metaAttrs;
        }

        protected StreamTransformer transformer() {
            return (ds, newAttrs, params) -> {
                final Map<ObjLvl, List<String>> attrs = (newAttrs != null)
                        ? newAttrs
                        : ((metaAttrs != null) ? metaAttrs : ds.attributes());

                VariablesContext thisCall = new VariablesContext(vc);
                for (String param : params.definitions()) {
                    thisCall.putHere(param, params.get(param));
                }
                for (Map.Entry<ObjLvl, List<String>> ola : attrs.entrySet()) {
                    thisCall.putHere(ATTRS_PREFIX + ola.getKey(), new ArrayWrap(ola.getValue()));
                }

                Broadcast<VariablesContext> broadVars = JavaSparkContext.fromSparkContext(ds.rdd().context()).broadcast(thisCall);
                Broadcast<List<StatementItem>> broadStmt = JavaSparkContext.fromSparkContext(ds.rdd().context()).broadcast(items);

                return new DataStreamBuilder(outputName, attrs)
                        .transformed(name, (resultType == StreamType.Passthru) ? ds.streamType : resultType, ds)
                        .build(ds.rdd()
                                .mapPartitionsWithIndex((idx, it) -> {
                                    List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                                    VariablesContext vars = new VariablesContext(broadVars.getValue());
                                    vars.putHere(PARTITION, idx);
                                    List<StatementItem> stmts = broadStmt.getValue();

                                    CallContext cc = new CallContext(it, ret);
                                    while (!cc.returnReached) {
                                        cc.eval(stmts, vars);
                                    }

                                    return ret.iterator();
                                }, true)
                                .mapToPair(t -> t)
                        );
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

                            vc.putHere(FETCH_VAR, false);
                        } else {
                            key = null;
                            rec = null;

                            vc.putHere(FETCH_VAR, true);
                        }

                        if (fi.control.length == 1) {
                            vc.putHere(fi.control[0], rec);
                        } else if (fi.control.length == 2) {
                            vc.putHere(fi.control[0], key);
                            vc.putHere(fi.control[1], rec);
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
                            loopValues = new ArrayWrap(expr).data();

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
                        break;
                    }
                }
            }
        }
    }
}
