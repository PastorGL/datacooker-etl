/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.AttrGetter;

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public interface Operator {
    static String popString(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof String) {
            return (String) a;
        }
        return String.valueOf(a);
    }

    static int popInt(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).intValue();
        }
        return Utils.parseNumber(String.valueOf(a)).intValue();
    }

    static long popLong(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).longValue();
        }
        return Utils.parseNumber(String.valueOf(a)).longValue();
    }

    static double popDouble(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).doubleValue();
        }
        return Utils.parseNumber(String.valueOf(a)).doubleValue();
    }

    static boolean popBoolean(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Boolean) {
            return (Boolean) a;
        }
        return Boolean.parseBoolean(String.valueOf(a));
    }

    static Object[] popArray(Deque<Object> args) {
        Object o = args.pop();

        if (o.getClass().isArray()) {
            return (Object[]) o;
        } else if (o instanceof Collection) {
            return ((Collection) o).toArray();
        } else {
            return new Object[]{o};
        }
    }

    static boolean peekNull(Deque<Object> args) {
        Object z = args.peek();
        return (z == null);
    }

    static boolean bool(AttrGetter props, List<Expression<?>> item, VariablesContext vc) {
        if ((item == null) || item.isEmpty()) {
            return true;
        }

        Object r = eval(props, item, vc);
        if (r == null) {
            return false;
        }

        return Boolean.parseBoolean(String.valueOf(r));
    }

    static Object eval(AttrGetter props, List<Expression<?>> item, VariablesContext vc) {
        if (item.isEmpty()) {
            return null;
        }

        Deque<Object> stack = new LinkedList<>();
        Deque<Object> top = null;
        for (Expression<?> ei : item) {
            // these all push to expression stack
            if (ei instanceof Expressions.PropItem) {
                stack.push(((Expressions.PropItem) ei).get(props));
                continue;
            }
            if (ei instanceof Expressions.VarItem) {
                stack.push(((Expressions.VarItem) ei).get(vc));
                continue;
            }
            if (ei instanceof Expressions.StringItem) {
                stack.push(((Expressions.StringItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.NumericItem) {
                stack.push(((Expressions.NumericItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.NullItem) {
                stack.push(((Expressions.NullItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.BoolItem) {
                stack.push(((Expressions.BoolItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.SetItem) {
                stack.push(((Expressions.SetItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.OpItem) {
                stack.push(((Expressions.OpItem) ei).eval(top));
                continue;
            }
            if (ei instanceof Expressions.IsExpr) {
                stack.push(((Expressions.IsExpr) ei).eval(top.pop()));
                continue;
            }
            if (ei instanceof Expressions.InExpr) {
                stack.push(((Expressions.InExpr) ei).eval(top.pop(), top.pop()));
                continue;
            }
            if (ei instanceof Expressions.BetweenExpr) {
                stack.push(((Expressions.BetweenExpr) ei).eval(top.pop()));
                continue;
            }
            // and this one pops from it
            if (ei instanceof Expressions.StackGetter) {
                top = ((Expressions.StackGetter) ei).get(stack);
                continue;
            }
        }

        if (stack.size() != 1) {
            throw new RuntimeException("Invalid SELECT item expression");
        }

        return stack.pop();
    }

    Object call(Deque<Object> args);

    String name();

    int ariness();
}
