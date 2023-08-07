/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.cli.repl.DSData;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;

import javax.inject.Inject;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
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
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> ds() {
        return new ArrayList<>(dc.getAll());
    }

    @GET
    @Path("info/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public DSData variable(@PathParam("name") @NotEmpty String name) {
        DataStream dataStream = dc.get(name);

        return new DSData(dataStream.accessor.attributes(), dataStream.rdd.getStorageLevel().description(),
                dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages());
    }

    @GET
    @Path("sample/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> sample(@PathParam("name") @NotEmpty String name, @QueryParam("size") @Positive @NotNull Integer size) {
        return dc.get(name).rdd.takeSample(false, size).stream()
                .map(r -> r._1 + " => " + r._2)
                .collect(Collectors.toList());
    }
}
