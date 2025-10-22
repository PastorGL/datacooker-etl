/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.FunctionInfo;
import io.github.pastorgl.datacooker.metadata.OperatorInfo;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.Pluggables;
import io.github.pastorgl.datacooker.scripting.Functions;
import io.github.pastorgl.datacooker.scripting.Operators;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotEmpty;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

import static io.github.pastorgl.datacooker.DataCooker.FUNCTIONS;
import static io.github.pastorgl.datacooker.DataCooker.TRANSFORMS;

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
    public PackageInfo registeredPackage(@QueryParam("name") @NotEmpty String name) {
        return RegisteredPackages.REGISTERED_PACKAGES.getOrDefault(name, null);
    }

    @GET
    @Path("input/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> input() {
        return new ArrayList<>(Pluggables.INPUTS.keySet());
    }

    @GET
    @Path("input")
    @Produces(MediaType.APPLICATION_JSON)
    public PluggableMeta input(@QueryParam("name") @NotEmpty String name) {
        return Pluggables.INPUTS.containsKey(name) ? Pluggables.INPUTS.get(name).meta : null;
    }

    @GET
    @Path("transform/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> transform() {
        ArrayList<String> all = new ArrayList<>(Pluggables.TRANSFORMS.keySet());
        all.addAll(TRANSFORMS.keySet());
        return all;
    }

    @GET
    @Path("transform")
    @Produces(MediaType.APPLICATION_JSON)
    public PluggableMeta transform(@QueryParam("name") @NotEmpty String name) {
        if (Pluggables.TRANSFORMS.containsKey(name)) {
            return Pluggables.TRANSFORMS.get(name).meta;
        }

        if (TRANSFORMS.containsKey(name)) {
            return TRANSFORMS.get(name).meta;
        }

        return null;
    }

    @GET
    @Path("operation/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> operation() {
        return new ArrayList<>(Pluggables.OPERATIONS.keySet());
    }

    @GET
    @Path("operation")
    @Produces(MediaType.APPLICATION_JSON)
    public PluggableMeta operation(@QueryParam("name") @NotEmpty String name) {
        return Pluggables.OPERATIONS.containsKey(name) ? Pluggables.OPERATIONS.get(name).meta : null;
    }

    @GET
    @Path("output/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> output() {
        return new ArrayList<>(Pluggables.OUTPUTS.keySet());
    }

    @GET
    @Path("output")
    @Produces(MediaType.APPLICATION_JSON)
    public PluggableMeta output(@QueryParam("name") @NotEmpty String name) {
        return Pluggables.OUTPUTS.containsKey(name) ? Pluggables.OUTPUTS.get(name).meta : null;
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
    public OperatorInfo operator(@QueryParam("name") @NotEmpty String name) {
        return Operators.OPERATORS.get(name);
    }

    @GET
    @Path("function/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> function() {
        ArrayList<String> all = new ArrayList<>(Functions.FUNCTIONS.keySet());
        all.addAll(FUNCTIONS.keySet());
        return all;
    }

    @GET
    @Path("function")
    @Produces(MediaType.APPLICATION_JSON)
    public FunctionInfo function(@QueryParam("name") @NotEmpty String name) {
        if (Functions.FUNCTIONS.containsKey(name)) {
            return Functions.FUNCTIONS.get(name);
        }

        if (FUNCTIONS.containsKey(name)) {
            return FUNCTIONS.get(name);
        }

        return null;
    }
}
