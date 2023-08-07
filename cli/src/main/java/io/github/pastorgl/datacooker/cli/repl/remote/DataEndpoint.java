/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;

import javax.inject.Inject;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
@Path("ds")
public class DataEndpoint {
    DataContext dc;

    @Inject
    public DataEndpoint(DataContext dc) {
        this.dc = dc;
    }

    @GET
    @Path("")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> ds() {
        return new ArrayList<>(dc.getAll());
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public DSData variable(@PathParam("name") @NotEmpty String name) {
        DataStream dataStream = dc.get(name);

        return new DSData(dataStream.accessor.attributes());
    }


    public static class DSData {
        public final Map<String, List<String>> attrs;

        public DSData(Map<String, List<String>> attrs) {
            this.attrs = attrs;
        }
    }
}
