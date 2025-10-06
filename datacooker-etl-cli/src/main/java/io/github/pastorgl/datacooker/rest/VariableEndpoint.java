/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.scripting.VariableInfo;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotEmpty;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.DataCooker.GLOBAL_VARS;

@Singleton
@Path("variable")
public class VariableEndpoint {
    @GET
    @Path("enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> variables() {
        return new ArrayList<>(GLOBAL_VARS.getAll());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public VariableInfo variable(@QueryParam("name") @NotEmpty String name) {
        return GLOBAL_VARS.varInfo(name);
    }
}
