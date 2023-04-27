/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class VariablesContext {
    private final Map<String, Object> holder = new HashMap<>();
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

            if (o instanceof Object[]) {
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

    public String getString(String varName) {
        return getString(varName, null);
    }

    public String getString(String varName, String defaults) {
        String ret = null;
        if (holder.containsKey(varName)) {
            ret = String.valueOf(holder.get(varName));
        }

        if (ret != null) {
            return ret;
        }

        return (parent == null) ? defaults : parent.getString(varName, defaults);
    }

    public Double getNumber(String varName) {
        return getNumber(varName, null);
    }

    public Double getNumber(String varName, Double defaults) {
        Double ret = null;
        if (holder.containsKey(varName)) {
            ret = Double.parseDouble(String.valueOf(holder.get(varName)));
        }

        if (ret != null) {
            return ret;
        }

        return (parent == null) ? defaults : parent.getNumber(varName, defaults);
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

    public void putAll(Map<String, Object> all) {
        holder.putAll(all);
    }
}
