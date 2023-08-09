/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.cli.repl.StreamInfo;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
}
