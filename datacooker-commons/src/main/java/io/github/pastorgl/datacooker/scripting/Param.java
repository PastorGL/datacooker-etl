package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;

public class Param {
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
