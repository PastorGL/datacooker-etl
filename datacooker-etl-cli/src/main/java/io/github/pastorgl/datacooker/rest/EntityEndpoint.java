/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.EvaluatorInfo;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.scripting.*;
import io.github.pastorgl.datacooker.storage.Adapters;
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
@Path("")
public class EntityEndpoint {
    final Library library;

    @Inject
    public EntityEndpoint(Library library) {
        this.library = library;
    }

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

    @GET
    @Path("operator/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> operator() {
        return new ArrayList<>(Operators.OPERATORS.keySet());
    }

    @GET
    @Path("operator")
    @Produces(MediaType.APPLICATION_JSON)
    public EvaluatorInfo operator(@QueryParam("name") @NotEmpty String name) {
        return EvaluatorInfo.operator(name);
    }

    @GET
    @Path("function/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> function() {
        ArrayList<String> all = new ArrayList<>(Functions.FUNCTIONS.keySet());
        all.addAll(library.functions.keySet());
        return all;
    }

    @GET
    @Path("function")
    @Produces(MediaType.APPLICATION_JSON)
    public EvaluatorInfo function(@QueryParam("name") @NotEmpty String name) {
        EvaluatorInfo function = EvaluatorInfo.function(name);
        if (function != null) {
            return function;
        }

        if (library.functions.containsKey(name)) {
            Function<?> func = library.functions.get(name);
            return new EvaluatorInfo(func.name(), func.descr(), func.arity());
        }

        return null;
    }
}
