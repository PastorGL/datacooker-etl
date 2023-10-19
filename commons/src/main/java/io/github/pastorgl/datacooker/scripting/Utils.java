/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.util.regex.Pattern;

public class Utils {
    private static final Pattern INT = Pattern.compile("[+\\-]?\\d+");

    public static Number parseNumber(String sqlNumeric) {
        String lNumeric = sqlNumeric.toLowerCase();
        if (lNumeric.endsWith("l")) {
            return Long.parseLong(sqlNumeric.substring(0, sqlNumeric.length() - 1));
        }
        if (lNumeric.startsWith("0x")) {
            return Long.parseUnsignedLong(sqlNumeric.substring(2), 16);
        }
        if (lNumeric.endsWith("h")) {
            return Long.parseUnsignedLong(sqlNumeric.substring(0, sqlNumeric.length() - 1), 16);
        }
        if (INT.matcher(sqlNumeric).matches()) {
            return Integer.parseInt(sqlNumeric);
        }
        return Double.parseDouble(sqlNumeric);
    }
}
