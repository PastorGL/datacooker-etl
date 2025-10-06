/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.data.DataHelper;
import io.github.pastorgl.datacooker.data.StreamLineage;
import io.github.pastorgl.datacooker.scripting.StreamInfo;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.DataCooker.DATA_CONTEXT;

@Singleton
@Path("ds")
public class DataEndpoint {
    @GET
    @Path("enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> ds() {
        return new ArrayList<>(DATA_CONTEXT.getWildcard());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public StreamInfo info(@QueryParam("name") @NotEmpty String name) {
        if (DATA_CONTEXT.has(name)) {
            return DATA_CONTEXT.streamInfo(name);
        }

        return null;
    }

    @GET
    @Path("sample")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> sample(@QueryParam("name") @NotEmpty String name,
                               @QueryParam("limit") @PositiveOrZero @NotNull Integer limit) {
        return DATA_CONTEXT.get(name).rdd().takeSample(false, limit).stream()
                .map(r -> r._1 + " => " + r._2)
                .collect(Collectors.toList());
    }

    @GET
    @Path("part")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> part(@QueryParam("name") @NotEmpty String name,
                             @QueryParam("part") @PositiveOrZero @NotNull Integer part,
                             @QueryParam("limit") @PositiveOrZero @NotNull Integer limit) {
        return DataHelper.takeFromPart(DATA_CONTEXT.get(name).rdd(), part, limit).collect(Collectors.toList());
    }

    @POST
    @Path("persist")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public StreamInfo persist(String name) {
        return DATA_CONTEXT.persist(name);
    }

    @GET
    @Path("renounce")
    @Produces(MediaType.APPLICATION_JSON)
    public String renounce(@QueryParam("name") @NotEmpty String name) {
        DATA_CONTEXT.renounce(name);
        return null;
    }

    @GET
    @Path("lineage")
    @Produces(MediaType.APPLICATION_JSON)
    public List<StreamLineage> lineage(@QueryParam("name") @NotEmpty String name) {
        return DATA_CONTEXT.get(name).lineage;
    }
}
