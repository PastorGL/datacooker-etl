/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

public class RaiseException extends RuntimeException {
    public RaiseException(String message) {
        super(message);
    }

    public RaiseException(String message, Throwable cause) {
        super(message, cause);
    }
}
