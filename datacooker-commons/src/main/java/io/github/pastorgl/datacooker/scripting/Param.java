/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.Serializable;

public class Param implements Serializable {
    public final boolean optional;
    public final Object defaults;

    @JsonCreator
    public Param(boolean optional, Object defaults) {
        this.optional = optional;
        this.defaults = defaults;
    }

    Param(Object defaults) {
        this.optional = true;
        this.defaults = defaults;
    }

    Param() {
        this.optional = false;
        this.defaults = null;
    }
}
