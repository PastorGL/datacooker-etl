/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.Structured;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class VariablesContext {
    private final Map<String, Object> holder = new TreeMap<>();

    final VariablesContext parent;
    final int level;

    public VariablesContext() {
        this.parent = null;
        this.level = 0;
    }

    public VariablesContext(VariablesContext parent) {
        this.parent = parent;
        this.level = parent.level + 1;
    }

    public ArrayWrap getArray(String varName) {
        ArrayWrap ret = null;
        if (holder.containsKey(varName)) {
            Object o = holder.get(varName);
            if (o == null) {
                return null;
            }

            ret = new ArrayWrap(o);
        }

        if (ret != null) {
            return ret;
        }

        return (parent == null) ? null : parent.getArray(varName);
    }

    public Object getVar(String varName) {
        String path = null;
        if (varName.contains(".")) {
            path = varName.substring(varName.indexOf("."));
            varName = varName.substring(0, varName.indexOf("."));
        }

        Object val = holder.get(varName);
        if ((val == null) && (parent != null)) {
            val = parent.getVar(varName);
        }

        if ((val != null) && (path != null)) {
            val = new Structured(val).asIs(path);
        }

        return val;
    }

    public void put(String varName, Object value) {
        if (value == null) {
            holder.remove(varName);
        } else {
            holder.put(varName, value);
        }
    }

    public Set<String> getAll() {
        return holder.keySet();
    }

    public void putAll(Map<String, Object> all) {
        holder.putAll(all);
    }

    public VariableInfo varInfo(String name) {
        return new VariableInfo(holder.get(name));
    }
}
