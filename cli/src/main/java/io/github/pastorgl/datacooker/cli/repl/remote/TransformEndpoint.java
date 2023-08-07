/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.TransformMeta;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("transform")
public class TransformEndpoint {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> transform() {
        return new ArrayList<>(Transforms.TRANSFORMS.keySet());
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public TransformMeta transform(@PathParam("name") @NotEmpty String name) {
        return Transforms.TRANSFORMS.get(name).meta;
    }
}
