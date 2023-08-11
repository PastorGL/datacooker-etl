/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;

public class VariableInfo {
    public final String className;
    public final String value;

    @JsonCreator
    public VariableInfo(String className, String value) {
        this.className = className;
        this.value = value;
    }

    public VariableInfo(Object value) {
        if (value != null) {
            this.className = value.getClass().getSimpleName();

            if (value.getClass().isArray()) {
                this.value = Arrays.toString((Object[]) value);
            } else {
                this.value = String.valueOf(value);
            }
        } else {
            this.className = "NULL";
            this.value = "NULL";
        }
    }
}
