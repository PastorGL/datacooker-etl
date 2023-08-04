/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.storage.Adapters;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("adapter")
public class AdapterEndpoint {
    @GET
    @Path("input")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> input() {
        return new ArrayList<>(Adapters.INPUTS.keySet());
    }

    @GET
    @Path("input/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public AdapterMeta input(@PathParam("name") @NotEmpty String name) {
        return Adapters.INPUTS.get(name).meta;
    }

    @GET
    @Path("output")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> output() {
        return new ArrayList<>(Adapters.OUTPUTS.keySet());
    }

    @GET
    @Path("output/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public AdapterMeta output(@PathParam("name") @NotEmpty String name) {
        return Adapters.OUTPUTS.get(name).meta;
    }
}
