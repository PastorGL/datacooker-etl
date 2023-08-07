/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.cli.Main;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.OptionsContext;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("meta")
public class MetaEndpoint {
    DataContext dc;
    OptionsContext oc;

    @Inject
    public MetaEndpoint(OptionsContext oc, DataContext dc) {
        this.oc = oc;
        this.dc = dc;
    }

    @GET
    @Path("alive")
    @Produces(MediaType.APPLICATION_JSON)
    public String alive() {
        return "ok";
    }


    @GET
    @Path("version")
    @Produces(MediaType.APPLICATION_JSON)
    public String version() {
        return Main.getVersion();
    }

    @OPTIONS
    @Path("options")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> options() {
        return new ArrayList<>(oc.getAll());
    }

    @OPTIONS
    @Path("options/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Object options(@PathParam("name") String name) {
        return oc.getOption(name);
    }
}
