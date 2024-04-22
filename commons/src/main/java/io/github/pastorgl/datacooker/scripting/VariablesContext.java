/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class VariablesContext {
    private final Map<String, Object> holder = new TreeMap<>();
    VariablesContext parent;

    public VariablesContext() {
    }

    public VariablesContext(VariablesContext parent) {
        this.parent = parent;
    }

    public Object[] getArray(String varName) {
        Object[] ret = null;
        if (holder.containsKey(varName)) {
            Object o = holder.get(varName);
            if (o == null) {
                return null;
            }

            if (o.getClass().isArray()) {
                ret = (Object[]) o;
            } else if (o instanceof Collection) {
                ret = ((Collection) o).toArray();
            } else {
                ret = new Object[]{o};
            }
        }

        if (ret != null) {
            return ret;
        }

        return (parent == null) ? null : parent.getArray(varName);
    }

    public Object getVar(String varName) {
        Object val = holder.get(varName);
        if ((val == null) && (parent != null)) {
            return parent.getVar(varName);
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
