/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.scripting.Procedure;
import io.github.pastorgl.datacooker.scripting.TDLErrorListener;
import io.github.pastorgl.datacooker.scripting.TDLInterpreter;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotEmpty;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

import static io.github.pastorgl.datacooker.DataCooker.GLOBAL_VARS;
import static io.github.pastorgl.datacooker.DataCooker.PROCEDURES;

@Singleton
@Path("exec")
public class ExecutorEndpoint {
    @PUT
    @Path("expr")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String expr(String expr) {
        TDLInterpreter tdl = new TDLInterpreter(expr, new TDLErrorListener());
        try {
            return String.valueOf(tdl.interpretExpr(GLOBAL_VARS));
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @POST
    @Path("script")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String script(String line) {
        TDLErrorListener errorListener = new TDLErrorListener();
        TDLInterpreter tdl = new TDLInterpreter(line, errorListener);
        tdl.interpret();
        return null;
    }

    @POST
    @Path("parse")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TDLErrorListener parse(String line) {
        TDLErrorListener errorListener = new TDLErrorListener();
        TDLInterpreter tdl = new TDLInterpreter(line, errorListener);
        tdl.parseScript();
        return errorListener;
    }

    @GET
    @Path("procedure")
    @Produces(MediaType.APPLICATION_JSON)
    public Procedure procedure(@QueryParam("name") @NotEmpty String name) {
        return PROCEDURES.getOrDefault(name, null);
    }

    @GET
    @Path("procedure/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> procedures() {
        return PROCEDURES.keySet().stream().toList();
    }
}
