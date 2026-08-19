/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

public enum MsgLvl {
    INFO, WARNING, ERROR;

    public static MsgLvl get(String lvlStr) {
        if (lvlStr == null) return ERROR;
        switch (lvlStr.toUpperCase()) {
            case "DEBUG": case "INFO": case "LOG": case "NOTICE": return INFO;
            case "WARN": case "WARNING": return WARNING;
            default: return ERROR;
        }
    }
}
