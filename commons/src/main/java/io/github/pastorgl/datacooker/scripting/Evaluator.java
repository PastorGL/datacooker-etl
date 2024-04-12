/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.io.Serializable;
import java.util.Collection;
import java.util.Deque;

public interface Evaluator<R> extends Serializable {
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

    R call(Deque<Object> args);

    String name();
    String descr();

    int arity();
}
