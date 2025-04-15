/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

public enum MsgLvl {
    INFO, WARNING, ERROR;

    public static MsgLvl get(String lvlStr) {
        return (lvlStr == null) ? ERROR : switch (lvlStr.toUpperCase()) {
            case "DEBUG", "INFO", "LOG", "NOTICE" -> INFO;
            case "WARN", "WARNING" -> WARNING;
            default -> ERROR;
        };
    }
}
