/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.Options;

import java.io.Serializable;
import java.util.*;

public class OptionsContext implements Serializable {
    private static Map<String, Object> holder;

    public static void initialize() {
        holder = new TreeMap<>();

        Arrays.stream(Options.values()).forEach(o -> {
            holder.put(o.name(), o.def());
        });
    }

    public static String getString(String optName) {
        if (holder.containsKey(optName)) {
            Object o = holder.get(optName);
            if (o != null) {
                return String.valueOf(holder.get(optName));
            }
        }

        return null;
    }

    public static Number getNumber(String optName) {
        if (holder.containsKey(optName)) {
            Object o = holder.get(optName);
            if (o != null) {
                return (Number) holder.get(optName);
            }
        }

        return null;
    }

    public static boolean getBoolean(String optName) {
        if (holder.containsKey(optName)) {
            Object o = holder.get(optName);
            if (o != null) {
                return Boolean.parseBoolean(String.valueOf(o));
            }
        }

        return false;
    }

    public static Object getOption(String optName) {
        return holder.get(optName);
    }

    public static void put(String optName, Object value) {
        holder.put(optName, value);
    }

    public static Set<String> getAll() {
        return holder.keySet();
    }

    public static void putAll(Map<String, Object> all) {
        holder.putAll(all);
    }

    public static String print() {
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
