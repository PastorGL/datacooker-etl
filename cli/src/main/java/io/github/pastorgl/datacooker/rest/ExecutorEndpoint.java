/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.OptionsContext;
import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;
import io.github.pastorgl.datacooker.scripting.TDL4Interpreter;
import io.github.pastorgl.datacooker.scripting.VariablesContext;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("exec")
public class ExecutorEndpoint {
    final DataContext dc;
    final VariablesContext vc;
    final OptionsContext oc;

    @Inject
    public ExecutorEndpoint(VariablesContext vc, DataContext dc, OptionsContext oc) {
        this.vc = vc;
        this.dc = dc;
        this.oc = oc;
    }

    @PUT
    @Path("expr")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String expr(String expr) {
        TDL4Interpreter tdl4 = new TDL4Interpreter(expr, vc, oc, new TDL4ErrorListener());
        try {
            return String.valueOf(tdl4.interpretExpr());
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @POST
    @Path("script")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String script(String line) {
        TDL4ErrorListener errorListener = new TDL4ErrorListener();
        TDL4Interpreter tdl4 = new TDL4Interpreter(line, vc, oc, errorListener);
        tdl4.interpret(dc);
        return null;
    }

    @POST
    @Path("parse")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TDL4ErrorListener parse(String line) {
        TDL4ErrorListener errorListener = new TDL4ErrorListener();
        TDL4Interpreter tdl4 = new TDL4Interpreter(line, vc, oc, errorListener);
        tdl4.parseScript();
        return errorListener;
    }
}
