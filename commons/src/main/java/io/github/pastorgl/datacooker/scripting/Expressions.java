/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.Record;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public final class Expressions {
    public interface ExprItem<T> extends Serializable {
    }

    @FunctionalInterface
    public interface SetItem extends ExprItem<Set<Object>> {
        Set<Object> get();
    }

    public static SetItem setItem(Object[] a) {
        return new SetItem() {
            @Override
            public Set<Object> get() {
                return (a == null) ? Collections.emptySet() : new HashSet<>(Arrays.asList(a));
            }

            @Override
            public String toString() {
                return "ARRAY[" + a.length + "]";
            }
        };
    }

    @FunctionalInterface
    public interface BetweenExpr extends ExprItem<Boolean> {
        boolean eval(final Object b);
    }

    public static BetweenExpr between(double l, double r) {
        return new BetweenExpr() {
            @Override
            public boolean eval(Object b) {
                double t = (b instanceof Number) ? ((Number) b).doubleValue() : Utils.parseNumber(String.valueOf(b)).doubleValue();
                return (t >= l) && (t <= r);
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
            public boolean eval(Object b) {
                double t = (b instanceof Number) ? ((Number) b).doubleValue() : Utils.parseNumber(String.valueOf(b)).doubleValue();
                return (t < l) || (t > r);
            }

            @Override
            public String toString() {
                return "NOT BETWEEN " + l + " AND " + r;
            }
        };
    }

    @FunctionalInterface
    public interface InExpr extends ExprItem<Boolean> {
        boolean eval(Object n, Object h);
    }

    public static InExpr in() {
        return new InExpr() {
            @Override
            public boolean eval(Object n, Object h) {
                if (n == null) {
                    return false;
                }

                Collection<?> haystack = null;
                if (h instanceof Collection) {
                    haystack = (Collection<?>) h;
                }
                if (h instanceof Object[]) {
                    haystack = Arrays.asList((Object[]) h);
                }
                if (haystack == null) {
                    return Objects.equals(n, h);
                }

                if (haystack.isEmpty()) {
                    return false;
                }
                Object item = haystack.iterator().next();
                if ((item != null) && Number.class.isAssignableFrom(item.getClass())) {
                    haystack = haystack.stream().map(e -> ((Number) e).doubleValue()).collect(Collectors.toList());
                    n = (n instanceof Number) ? ((Number) n).doubleValue() : Utils.parseNumber(String.valueOf(n)).doubleValue();
                }
                return haystack.contains(n);
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
            public boolean eval(Object n, Object h) {
                if (n == null) {
                    return false;
                }

                Collection<?> haystack = null;
                if (h instanceof Collection) {
                    haystack = (Collection<?>) h;
                }
                if (h instanceof Object[]) {
                    haystack = Arrays.asList((Object[]) h);
                }
                if (haystack == null) {
                    return !Objects.equals(n, h);
                }

                if (haystack.isEmpty()) {
                    return true;
                }
                Object item = haystack.iterator().next();
                if ((item != null) && Number.class.isAssignableFrom(item.getClass())) {
                    haystack = haystack.stream().map(e -> ((Number) e).doubleValue()).collect(Collectors.toList());
                    n = (n instanceof Number) ? ((Number) n).doubleValue() : Utils.parseNumber(String.valueOf(n)).doubleValue();
                }
                return !haystack.contains(n);
            }

            @Override
            public String toString() {
                return "NOT IN";
            }
        };
    }

    @FunctionalInterface
    public interface IsExpr extends ExprItem<Boolean> {
        boolean eval(Object rv);
    }

    public static IsExpr isNull() {
        return new IsExpr() {
            @Override
            public boolean eval(Object obj) {
                return Objects.isNull(obj);
            }

            @Override
            public String toString() {
                return "IS NULL";
            }
        };
    }

    public static IsExpr nonNull() {
        return Objects::nonNull;
    }

    @FunctionalInterface
    public interface AttrItem extends ExprItem<Record<?>> {
        Object get(Record<?> obj);
    }

    public static AttrItem attrItem(String attr) {
        return new AttrItem() {
            @Override
            public Object get(Record<?> r) {
                return r.asIs(attr);
            }

            @Override
            public String toString() {
                return attr;
            }
        };
    }

    @FunctionalInterface
    public interface ObjItem extends ExprItem<Object> {
        boolean recOrKey();
    }

    public static ObjItem recItem() {
        return new ObjItem() {
            @Override
            public boolean recOrKey() {
                return true;
            }

            @Override
            public String toString() {
                return "<Record>";
            }
        };
    }

    public static ObjItem keyItem() {
        return new ObjItem() {
            @Override
            public boolean recOrKey() {
                return false;
            }

            @Override
            public String toString() {
                return "<Key>";
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

    public static VarItem arrItem(String varName) {
        return new VarItem() {
            @Override
            public Object get(VariablesContext vc) {
                return vc.getArray(varName);
            }

            @Override
            public String toString() {
                return "[ $" + varName + " ]";
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

    public static boolean boolLoose(List<ExprItem<?>> item, VariablesContext vc) {
        return boolAttr(null, null, item, vc);
    }

    public static boolean boolAttr(Object key, Record<?> rec, List<ExprItem<?>> item, VariablesContext vc) {
        if ((item == null) || item.isEmpty()) {
            return true;
        }

        Object r = evalAttr(key, rec, item, vc);
        if (r == null) {
            return false;
        }

        return Boolean.parseBoolean(String.valueOf(r));
    }

    public static Object evalLoose(List<ExprItem<?>> item, VariablesContext vc) {
        return evalAttr(null, null, item, vc);
    }

    public static Object evalAttr(Object key, Record<?> rec, List<ExprItem<?>> item, VariablesContext vc) {
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
            if (ei instanceof SetItem) {
                stack.push(((SetItem) ei).get());
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
            if (ei instanceof ObjItem) {
                top = new LinkedList<>();
                top.add(((ObjItem) ei).recOrKey() ? rec : key);
                continue;
            }
        }

        if (stack.size() != 1) {
            throw new RuntimeException("Invalid TDL Expression");
        }

        return stack.pop();
    }
}
