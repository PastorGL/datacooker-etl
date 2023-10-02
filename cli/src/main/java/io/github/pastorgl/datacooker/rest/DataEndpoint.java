/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.cli.repl.StreamInfo;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
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
            DataStream dataStream = dc.get(name);

            return new StreamInfo(dataStream.accessor.attributes(), dataStream.rdd.getStorageLevel().description(),
                    dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages());
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

    @GET
    @Path("renounce")
    @Produces(MediaType.APPLICATION_JSON)
    public String renounce(@QueryParam("name") @NotEmpty String name) {
        dc.renounce(name);
        return null;
    }
}
