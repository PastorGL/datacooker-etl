/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.Structured;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static io.github.pastorgl.datacooker.Constants.ENV_VAR_PREFIX;
import static io.github.pastorgl.datacooker.Constants.OPT_VAR_PREFIX;
import static io.github.pastorgl.datacooker.DataCooker.OPTIONS_CONTEXT;

public class VariablesContext implements Serializable {
    private Map<String, Object> holder;

    final VariablesContext parent;
    final int level;

    public void initialize() {
        this.holder = new TreeMap<>();
    }

    public VariablesContext() {
        this.parent = null;
        this.level = 0;
    }

    public VariablesContext(VariablesContext parent) {
        this.parent = parent;
        this.level = parent.level + 1;
        this.holder = new TreeMap<>();
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
        if (varName.startsWith(OPT_VAR_PREFIX)) {
            return OPTIONS_CONTEXT.getOption(varName.substring(OPT_VAR_PREFIX.length()));
        }

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

    public void putHere(String varName, Object value) {
        if (varName.startsWith(OPT_VAR_PREFIX) || varName.startsWith(ENV_VAR_PREFIX)) {
            return;
        }

        if (value == null) {
            holder.remove(varName);
        } else {
            holder.put(varName, value);
        }
    }

    public void put(String varName, Object value) {
        if (varName.startsWith(OPT_VAR_PREFIX) || varName.startsWith(ENV_VAR_PREFIX)) {
            return;
        }

        if (value == null) {
            if (holder.containsKey(varName)) {
                holder.remove(varName);
            } else {
                if (parent != null) {
                    parent.put(varName, null);
                }
            }
        } else {
            if (holder.containsKey(varName)) {
                holder.put(varName, value);
            } else {
                VariablesContext toPut = this;

                while ((toPut != null) && !toPut.holder.containsKey(varName)) {
                    toPut = toPut.parent;
                }

                if (toPut != null) {
                    toPut.holder.put(varName, value);
                } else {
                    holder.put(varName, value);
                }
            }
        }
    }

    public Set<String> getAll() {
        TreeSet<String> all = new TreeSet<>(holder.keySet());
        OPTIONS_CONTEXT.getAll().forEach(o -> all.add(OPT_VAR_PREFIX + o));
        return all;
    }

    public void putAll(Map<String, Object> all) {
        holder.putAll(all);
    }

    public VariableInfo varInfo(String name) {
        return new VariableInfo(getVar(name));
    }
}
