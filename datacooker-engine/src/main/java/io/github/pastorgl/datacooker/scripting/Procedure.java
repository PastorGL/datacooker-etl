/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class Procedure {
    public final String descr;

    @JsonIgnore
    public final TDL.StatementsContext ctx;

    public final Map<String, Param> params;

    private Procedure(String descr, TDL.StatementsContext ctx, Map<String, Param> params) {
        this.descr = descr;
        this.ctx = ctx;
        this.params = params;
    }

    @JsonCreator
    public Procedure(String descr, Map<String, Param> params) {
        this.descr = descr;
        this.params = params;
        this.ctx = null;
    }

    public static Builder builder(String descr, TDL.StatementsContext ctx) {
        return new Builder(descr, ctx);
    }

    public static class Builder extends ParamsBuilder<Builder> {
        private final String descr;
        private final TDL.StatementsContext ctx;

        private Builder(String descr, TDL.StatementsContext ctx) {
            this.descr = descr;
            this.ctx = ctx;
        }

        public Procedure build() {
            return new Procedure(descr, ctx, params);
        }
    }
}
