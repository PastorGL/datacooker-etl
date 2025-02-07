/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

public enum UnionSpec {
    CONCAT,
    XOR,
    AND;

    public static UnionSpec get(String text) {
        if (text != null) {
            text = text.toUpperCase();

            if (text.contains("XOR")) {
                return XOR;
            }
            if (text.contains("AND")) {
                return AND;
            }
            return CONCAT;
        }

        return null;
    }
}
