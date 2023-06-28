/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.AttrGetter;

import java.util.*;

public final class Expressions {
    @FunctionalInterface
    public interface SetItem extends Expression<Set<Object>> {
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
    public interface BetweenExpr extends Expression<Boolean> {
        boolean eval(final Object b);
    }

    public static BetweenExpr between(double l, double r) {
        return new BetweenExpr() {
            @Override
            public boolean eval(Object b) {
                double t = (b instanceof Number) ? ((Number) b).doubleValue() : Double.parseDouble(String.valueOf(b));
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
                double t = (b instanceof Number) ? ((Number) b).doubleValue() : Double.parseDouble(String.valueOf(b));
                return (t < l) || (t > r);
            }

            @Override
            public String toString() {
                return "NOT BETWEEN " + l + " AND " + r;
            }
        };
    }

    @FunctionalInterface
    public interface InExpr extends Expression<Boolean> {
        boolean eval(Object n, Object h);
    }

    public static InExpr in() {
        return new InExpr() {
            @Override
            public boolean eval(Object n, Object h) {
                if (h instanceof Collection) {
                    return ((Collection<?>) h).contains(n);
                }
                if (h instanceof Object[]) {
                    return Arrays.asList((Object[]) h).contains(n);
                }
                return Objects.equals(n, h);
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
                if (h instanceof Collection) {
                    return !((Collection<?>) h).contains(n);
                }
                if (h instanceof Object[]) {
                    return !Arrays.asList((Object[]) h).contains(n);
                }
                return !Objects.equals(n, h);
            }

            @Override
            public String toString() {
                return "NOT IN";
            }
        };
    }

    @FunctionalInterface
    public interface IsExpr extends Expression<Boolean> {
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
    public interface PropItem extends Expression<Object> {
        Object get(AttrGetter obj);
    }

    public static PropItem propItem(String propName) {
        return new PropItem() {
            @Override
            public Object get(AttrGetter r) {
                return r.get(propName);
            }

            @Override
            public String toString() {
                return propName;
            }
        };
    }

    @FunctionalInterface
    public interface VarItem extends Expression<Object> {
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
    public interface StringItem extends Expression<String> {
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
    public interface NumericItem extends Expression<Number> {
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
    public interface NullItem extends Expression<Void> {
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
    public interface OpItem extends Expression<Object> {
        Object eval(Deque<Object> args);
    }

    public static OpItem opItem(Operator op) {
        return new OpItem() {
            @Override
            public Object eval(Deque<Object> args) {
                return op.op(args);
            }

            @Override
            public String toString() {
                return op.name();
            }
        };
    }

    @FunctionalInterface
    public interface StackGetter extends Expression<Deque<Object>> {
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
    public interface BoolItem extends Expression<Boolean> {
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
}
