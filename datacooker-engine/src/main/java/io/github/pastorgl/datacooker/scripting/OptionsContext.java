/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.Options;

import java.io.Serializable;
import java.util.*;

public class OptionsContext implements Serializable {
    private Map<String, Object> holder;

    public void initialize() {
        holder = new TreeMap<>();

        Arrays.stream(Options.values()).forEach(o -> {
            holder.put(o.name(), o.def());
        });
    }

    public String getString(String optName) {
        if (holder.containsKey(optName)) {
            Object o = holder.get(optName);
            if (o != null) {
                return String.valueOf(holder.get(optName));
            }
        }

        return null;
    }

    public Number getNumber(String optName) {
        if (holder.containsKey(optName)) {
            Object o = holder.get(optName);
            if (o != null) {
                return (Number) holder.get(optName);
            }
        }

        return null;
    }

    public boolean getBoolean(String optName) {
        if (holder.containsKey(optName)) {
            Object o = holder.get(optName);
            if (o != null) {
                return Boolean.parseBoolean(String.valueOf(o));
            }
        }

        return false;
    }

    public Object getOption(String optName) {
        return holder.get(optName);
    }

    public void put(String optName, Object value) {
        holder.put(optName, value);
    }

    public Set<String> getAll() {
        return holder.keySet();
    }

    public void putAll(Map<String, Object> all) {
        holder.putAll(all);
    }

    public String print() {
        List<String> sb = new LinkedList<>();
        sb.add(holder.size() + " set");
        Arrays.stream(Options.values()).forEach(o -> {
            String opt = o.name();

            sb.add(opt +
                    (!Objects.equals(holder.get(opt), o.def()) ? " set to: " + holder.get(opt) : " defaults to: " + o.def())
            );
        });

        return String.join("\n\t", sb);
    }
}
