/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.DataRecord;

import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public final class Expressions {
    public interface ExprItem<T> extends Serializable {
    }

    @FunctionalInterface
    public interface ArrayItem extends ExprItem<ArrayWrap> {
        ArrayWrap get();
    }

    public static ArrayItem arrayItem(ArrayWrap a) {
        return new ArrayItem() {
            @Override
            public ArrayWrap get() {
                return (a == null) ? new ArrayWrap() : a;
            }

            @Override
            public String toString() {
                return "ARRAY[" + ((a == null) ? "0" : a.length()) + "]";
            }
        };
    }

    @FunctionalInterface
    public interface BetweenExpr extends ExprItem<Boolean> {
        Boolean eval(final Object b);
    }

    public static BetweenExpr between(double l, double r) {
        return new BetweenExpr() {
            @Override
            public Boolean eval(Object b) {
                LinkedList<Object> args = new LinkedList<>();
                args.add(b);
                args.add(l);
                args.add(r);
                return Operators.BETWEEN.call(args);
            }

            @Override
            public String toString() {
                return "BETWEEN " + l + " AND " + r;
            }
        };
    }

    public static BetweenExpr notBetween(double l, double r) {
        return new BetweenExpr() {
            @Override
            public Boolean eval(Object b) {
                LinkedList<Object> args = new LinkedList<>();
                args.add(b);
                args.add(l);
                args.add(r);
                return !Operators.BETWEEN.call(args);
            }

            @Override
            public String toString() {
                return "NOT BETWEEN " + l + " AND " + r;
            }
        };
    }

    @FunctionalInterface
    public interface InExpr extends ExprItem<Boolean> {
        Boolean eval(Object n, Object h);
    }

    public static InExpr in() {
        return new InExpr() {
            @Override
            public Boolean eval(Object n, Object h) {
                LinkedList<Object> args = new LinkedList<>();
                args.add(n);
                args.add(h);
                return Operators.IN.call(args);
            }

            @Override
            public String toString() {
                return "IN";
            }
        };
    }

    public static InExpr notIn() {
        return new InExpr() {
            @Override
            public Boolean eval(Object n, Object h) {
                LinkedList<Object> args = new LinkedList<>();
                args.add(n);
                args.add(h);
                return !Operators.IN.call(args);
            }

            @Override
            public String toString() {
                return "NOT IN";
            }
        };
    }

    @FunctionalInterface
    public interface IsExpr extends ExprItem<Boolean> {
        Boolean eval(Object rv);
    }

    public static IsExpr isNull() {
        return new IsExpr() {
            @Override
            public Boolean eval(Object obj) {
                LinkedList<Object> args = new LinkedList<>();
                args.add(obj);
                return Operators.IS.call(args);
            }

            @Override
            public String toString() {
                return "IS NULL";
            }
        };
    }

    public static IsExpr isNotNull() {
        return new IsExpr() {
            @Override
            public Boolean eval(Object obj) {
                LinkedList<Object> args = new LinkedList<>();
                args.add(obj);
                return !Operators.IS.call(args);
            }

            @Override
            public String toString() {
                return "IS NOT NULL";
            }
        };
    }

    @FunctionalInterface
    public interface AttrItem extends ExprItem<DataRecord<?>> {
        Object get(DataRecord<?> obj);
    }

    public static AttrItem attrItem(String attr) {
        return new AttrItem() {
            @Override
            public Object get(DataRecord<?> r) {
                return r.asIs(attr);
            }

            @Override
            public String toString() {
                return attr;
            }
        };
    }

    @FunctionalInterface
    public interface RecItem extends ExprItem<Object> {
        Deque<Object> get(Deque<Object> stack);
    }

    public static RecItem objItem(int argc) {
        return new RecItem() {
            @Override
            public Deque<Object> get(Deque<Object> stack) {
                Deque<Object> top = new LinkedList<>();
                for (int i = 0; i < argc; i++) {
                    top.push(stack.pop());
                }
                top.push(true);
                return top;
            }

            @Override
            public String toString() {
                return "<Object> " + argc;
            }
        };
    }

    public static RecItem keyItem(int argc) {
        return new RecItem() {
            @Override
            public Deque<Object> get(Deque<Object> stack) {
                Deque<Object> top = new LinkedList<>();
                for (int i = 0; i < argc; i++) {
                    top.push(stack.pop());
                }
                top.push(false);
                return top;
            }

            @Override
            public String toString() {
                return "<Key> " + argc;
            }
        };
    }

    public static RecItem recItem(int argc) {
        return new RecItem() {
            @Override
            public Deque<Object> get(Deque<Object> stack) {
                Deque<Object> top = new LinkedList<>();
                for (int i = 0; i < argc; i++) {
                    top.push(stack.pop());
                }
                top.push(null);
                return top;
            }

            @Override
            public String toString() {
                return "<Record> " + argc;
            }
        };
    }

    @FunctionalInterface
    public interface VarItem extends ExprItem<Object> {
        Object get(VariablesContext vc);
    }

    public static VarItem varItem(String varName) {
        return new VarItem() {
            @Override
            public Object get(VariablesContext vc) {
                return vc.getVar(varName);
            }

            @Override
            public String toString() {
                return "$" + varName;
            }
        };
    }

    @FunctionalInterface
    public interface StringItem extends ExprItem<String> {
        String get();
    }

    public static StringItem stringItem(String immediate) {
        return new StringItem() {
            @Override
            public String get() {
                return immediate;
            }

            @Override
            public String toString() {
                return "'" + immediate + "'";
            }
        };
    }

    @FunctionalInterface
    public interface NumericItem extends ExprItem<Number> {
        Number get();
    }

    public static NumericItem numericItem(Number immediate) {
        return new NumericItem() {
            @Override
            public Number get() {
                return immediate;
            }

            @Override
            public String toString() {
                return immediate.toString();
            }
        };
    }

    @FunctionalInterface
    public interface NullItem extends ExprItem<Void> {
        Void get();
    }

    public static NullItem nullItem() {
        return new NullItem() {
            @Override
            public Void get() {
                return null;
            }

            @Override
            public String toString() {
                return "NULL";
            }
        };
    }

    @FunctionalInterface
    public interface OpItem extends ExprItem<Object> {
        Object eval(Deque<Object> args);
    }

    public static OpItem opItem(Operator<?> op) {
        return new OpItem() {
            @Override
            public Object eval(Deque<Object> args) {
                return op.call(args);
            }

            @Override
            public String toString() {
                return op.name();
            }
        };
    }

    public static OpItem funcItem(Function<?> func) {
        return new OpItem() {
            @Override
            public Object eval(Deque<Object> args) {
                return func.call(args);
            }

            @Override
            public String toString() {
                return func.name() + "()";
            }
        };
    }

    @FunctionalInterface
    public interface StackGetter extends ExprItem<Deque<Object>> {
        Deque<Object> get(Deque<Object> stack);
    }

    public static StackGetter stackGetter(int num) {
        return new StackGetter() {
            @Override
            public Deque<Object> get(Deque<Object> stack) {
                Deque<Object> top = new LinkedList<>();
                for (int i = 0; i < num; i++) {
                    top.push(stack.pop());
                }
                return top;
            }

            @Override
            public String toString() {
                return "POP " + num;
            }
        };
    }

    @FunctionalInterface
    public interface BoolItem extends ExprItem<Boolean> {
        Boolean get();
    }

    public static BoolItem boolItem(boolean immediate) {
        return new BoolItem() {
            @Override
            public Boolean get() {
                return immediate;
            }

            @Override
            public String toString() {
                return immediate ? "TRUE" : "FALSE";
            }
        };
    }

    public static boolean bool(Object key, DataRecord<?> rec, List<ExprItem<?>> item, VariablesContext vc) {
        if ((item == null) || item.isEmpty()) {
            return true;
        }

        Object r = eval(key, rec, item, vc);
        if (r == null) {
            return false;
        }

        return Boolean.parseBoolean(String.valueOf(r));
    }

    public static Object eval(Object key, DataRecord<?> rec, List<ExprItem<?>> item, VariablesContext vc) {
        if (item.isEmpty()) {
            return null;
        }

        Deque<Object> stack = new LinkedList<>();
        Deque<Object> top = null;
        for (ExprItem<?> ei : item) {
            // these all push to expression stack
            if (ei instanceof AttrItem) {
                stack.push(((AttrItem) ei).get(rec));
                continue;
            }
            if (ei instanceof VarItem) {
                stack.push(((VarItem) ei).get(vc));
                continue;
            }
            if (ei instanceof StringItem) {
                stack.push(((StringItem) ei).get());
                continue;
            }
            if (ei instanceof NumericItem) {
                stack.push(((NumericItem) ei).get());
                continue;
            }
            if (ei instanceof NullItem) {
                stack.push(((NullItem) ei).get());
                continue;
            }
            if (ei instanceof BoolItem) {
                stack.push(((BoolItem) ei).get());
                continue;
            }
            if (ei instanceof ArrayItem) {
                stack.push(((ArrayItem) ei).get());
                continue;
            }
            if (ei instanceof OpItem) {
                stack.push(((OpItem) ei).eval(top));
                continue;
            }
            if (ei instanceof IsExpr) {
                stack.push(((IsExpr) ei).eval(top.pop()));
                continue;
            }
            if (ei instanceof InExpr) {
                stack.push(((InExpr) ei).eval(top.pop(), top.pop()));
                continue;
            }
            if (ei instanceof BetweenExpr) {
                stack.push(((BetweenExpr) ei).eval(top.pop()));
                continue;
            }
            // and this one pops from it
            if (ei instanceof StackGetter) {
                top = ((StackGetter) ei).get(stack);
                continue;
            }
            // special case
            if (ei instanceof RecItem) {
                top = ((RecItem) ei).get(stack);
                Boolean b = (Boolean) top.pop();
                if (b == null) {
                    top.push(rec);
                    top.push(key);
                } else {
                    top.push(b ? rec : key);
                }
                continue;
            }
        }

        if (stack.size() != 1) {
            throw new RuntimeException("Invalid TDL Expression");
        }

        return stack.pop();
    }
}
