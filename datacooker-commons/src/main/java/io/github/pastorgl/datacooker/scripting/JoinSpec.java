/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

public enum JoinSpec {
    INNER,
    LEFT,
    RIGHT,
    OUTER,
    LEFT_ANTI,
    RIGHT_ANTI;

    public static JoinSpec get(String text) {
        if (text != null) {
            text = text.toUpperCase();

            if (text.contains("LEFT")) {
                return text.contains("ANTI") ? LEFT_ANTI : LEFT;
            }
            if (text.contains("RIGHT")) {
                return text.contains("ANTI") ? RIGHT_ANTI : RIGHT;
            }
            if (text.contains("OUTER")) {
                return OUTER;
            }
            return INNER;
        }

        return null;
    }
}
