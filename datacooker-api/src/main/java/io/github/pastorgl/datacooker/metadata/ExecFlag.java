/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

public enum ExecFlag {
    OPERATION("Operation"),
    TRANSFORM("Transform"),
    INPUT("Input Adapter"),
    OUTPUT("Output Adapter");

    private final String friendlyName;

    ExecFlag(String friendlyName) {
        this.friendlyName = friendlyName;
    }

    @Override
    public String toString() {
        return friendlyName;
    }
}
