/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OptionsContext {
    private final Map<String, Object> holder = new HashMap<>();

    public OptionsContext() {
    }

    public OptionsContext(Map<String, Object> opts) {
        holder.putAll(opts);
    }

    public Object[] getArray(String optName) {
        Object[] ret = null;
        if (holder.containsKey(optName)) {
            Object o = holder.get(optName);
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

        return null;
    }

    public String getString(String optName) {
        return getString(optName, null);
    }

    public String getString(String optName, String defaults) {
        String ret = null;
        if (holder.containsKey(optName)) {
            ret = String.valueOf(holder.get(optName));
        }

        if (ret != null) {
            return ret;
        }

        return defaults;
    }

    public Double getNumber(String optName) {
        return getNumber(optName, null);
    }

    public Double getNumber(String optName, Object defaults) {
        if (holder.containsKey(optName)) {
            if (holder.get(optName) != null) {
                return Double.parseDouble(String.valueOf(holder.get(optName)));
            } else {
                return null;
            }
        }

        if (defaults != null) {
            return Double.parseDouble(String.valueOf(defaults));
        }

        return null;
    }

    public Object getOption(String optName) {
        return holder.get(optName);
    }

    public void put(String optName, Object value) {
        if (value == null) {
            holder.remove(optName);
        } else {
            holder.put(optName, value);
        }
    }

    public Set<String> getAll() {
        return holder.keySet();
    }

    public void putAll(Map<String, Object> all) {
        holder.putAll(all);
    }

    public boolean getBoolean(String optName, String def) {
        if (holder.containsKey(optName)) {
            if (holder.get(optName) != null) {
                return Boolean.parseBoolean(String.valueOf(holder.get(optName)));
            }
        }

        if (def != null) {
            return Boolean.parseBoolean(def);
        }

        return false;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(holder);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
