/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class Procedure {
    @JsonIgnore
    public final TDL.StatementsContext ctx;

    public final Map<String, Param> params;

    private Procedure(TDL.StatementsContext ctx, Map<String, Param> params) {
        this.ctx = ctx;
        this.params = params;
    }

    @JsonCreator
    public Procedure(Map<String, Param> params) {
        this.params = params;
        this.ctx = null;
    }

    public static Builder builder(TDL.StatementsContext ctx) {
        return new Builder(ctx);
    }

    public static class Builder extends ParamsBuilder<Builder> {
        private final TDL.StatementsContext ctx;

        private Builder(TDL.StatementsContext ctx) {
            this.ctx = ctx;
        }

        public Builder mandatory(String name) {
            return super.mandatory(name);
        }

        public Builder optional(String name, Object value) {
            return super.optional(name, new Param(value));
        }

        public Procedure build() {
            return new Procedure(ctx, params);
        }
    }
}
