/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.StreamLineage;
import io.github.pastorgl.datacooker.scripting.StreamInfo;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
@Path("ds")
public class DataEndpoint {
    DataContext dc;

    @Inject
    public DataEndpoint(DataContext dc) {
        this.dc = dc;
    }

    @GET
    @Path("enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> ds() {
        return new ArrayList<>(dc.getAll());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public StreamInfo info(@QueryParam("name") @NotEmpty String name) {
        if (dc.has(name)) {
            return dc.streamInfo(name);
        }

        return null;
    }

    @GET
    @Path("sample")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> sample(@QueryParam("name") @NotEmpty String name, @QueryParam("limit") @Positive @NotNull Integer limit) {
        return dc.get(name).rdd.takeSample(false, limit).stream()
                .map(r -> r._1 + " => " + r._2)
                .collect(Collectors.toList());
    }

    @POST
    @Path("persist")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public StreamInfo persist(String name) {
        return dc.persist(name);
    }

    @GET
    @Path("renounce")
    @Produces(MediaType.APPLICATION_JSON)
    public String renounce(@QueryParam("name") @NotEmpty String name) {
        dc.renounce(name);
        return null;
    }

    @GET
    @Path("lineage")
    @Produces(MediaType.APPLICATION_JSON)
    public List<StreamLineage> lineage(@QueryParam("name") @NotEmpty String name) {
        return dc.get(name).lineage;
    }
}
