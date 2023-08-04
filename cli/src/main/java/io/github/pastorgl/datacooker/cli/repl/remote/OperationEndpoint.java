/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.scripting.Operations;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("operation")
public class OperationEndpoint {
    @GET
    @Path("")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> operation() {
        return new ArrayList<>(Operations.OPERATIONS.keySet());
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public OperationMeta operation(@PathParam("name") @NotEmpty String name) {
        return Operations.OPERATIONS.get(name).meta;
    }
}
