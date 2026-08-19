/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

public enum Partitioning {
    HASHCODE,
    RANDOM,
    SOURCE;

    public static Partitioning get(String text) {
        if (text == null) return HASHCODE;
        switch (text.toUpperCase()) {
            case "RANDOM": return RANDOM;
            case "SOURCE": return SOURCE;
            default: return HASHCODE;
        }
    }
}
