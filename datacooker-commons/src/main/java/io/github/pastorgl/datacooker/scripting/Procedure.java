/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

public class Procedure {
    @JsonIgnore
    public final TDL4.StatementsContext ctx;

    public final Map<String, Param> params;

    private Procedure(TDL4.StatementsContext ctx, Map<String, Param> params) {
        this.ctx = ctx;
        this.params = params;
    }

    @JsonCreator
    public Procedure(Map<String, Param> params) {
        this.params = params;
        this.ctx = null;
    }

    public static Builder builder(TDL4.StatementsContext ctx) {
        return new Builder(ctx);
    }

    public static class Builder {
        private final TDL4.StatementsContext ctx;
        private final Map<String, Param> params = new HashMap<>();

        private Builder(TDL4.StatementsContext ctx) {
            this.ctx = ctx;
        }

        public Builder mandatory(String name) {
            params.put(name, new Param());
            return this;
        }

        public Builder optional(String name, Object value) {
            params.put(name, new Param(value));
            return this;
        }

        public Procedure build() {
            return new Procedure(ctx, params);
        }
    }
}
