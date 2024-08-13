/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.scripting.VariableInfo;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotEmpty;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("variable")
public class VariableEndpoint {
    final VariablesContext vc;

    @Inject
    public VariableEndpoint(VariablesContext vc) {
        this.vc = vc;
    }

    @GET
    @Path("enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> variables() {
        return new ArrayList<>(vc.getAll());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public VariableInfo variable(@QueryParam("name") @NotEmpty String name) {
        return vc.varInfo(name);
    }
}
