/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.scripting.Operations;
import io.github.pastorgl.datacooker.storage.Adapters;

import javax.inject.Singleton;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("")
public class EntityEndpoint {
    @GET
    @Path("package/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> registeredPackage() {
        return new ArrayList<>(RegisteredPackages.REGISTERED_PACKAGES.keySet());
    }

    @GET
    @Path("package")
    @Produces(MediaType.APPLICATION_JSON)
    public String registeredPackage(@QueryParam("name") @NotEmpty String name) {
        return RegisteredPackages.REGISTERED_PACKAGES.getOrDefault(name, null);
    }

    @GET
    @Path("input/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> input() {
        return new ArrayList<>(Adapters.INPUTS.keySet());
    }

    @GET
    @Path("input")
    @Produces(MediaType.APPLICATION_JSON)
    public AdapterMeta input(@QueryParam("name") @NotEmpty String name) {
        return Adapters.INPUTS.containsKey(name) ? Adapters.INPUTS.get(name).meta : null;
    }

    @GET
    @Path("transform/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> transform() {
        return new ArrayList<>(Transforms.TRANSFORMS.keySet());
    }

    @GET
    @Path("transform")
    @Produces(MediaType.APPLICATION_JSON)
    public TransformMeta transform(@QueryParam("name") @NotEmpty String name) {
        return Transforms.TRANSFORMS.containsKey(name) ? Transforms.TRANSFORMS.get(name).meta : null;
    }

    @GET
    @Path("operation/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> operation() {
        return new ArrayList<>(Operations.OPERATIONS.keySet());
    }

    @GET
    @Path("operation")
    @Produces(MediaType.APPLICATION_JSON)
    public OperationMeta operation(@QueryParam("name") @NotEmpty String name) {
        return Operations.OPERATIONS.containsKey(name) ? Operations.OPERATIONS.get(name).meta : null;
    }

    @GET
    @Path("output/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> output() {
        return new ArrayList<>(Adapters.OUTPUTS.keySet());
    }

    @GET
    @Path("output")
    @Produces(MediaType.APPLICATION_JSON)
    public AdapterMeta output(@QueryParam("name") @NotEmpty String name) {
        return Adapters.OUTPUTS.containsKey(name) ? Adapters.OUTPUTS.get(name).meta : null;
    }
}