/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.cli.repl.OptionsInfo;
import io.github.pastorgl.datacooker.scripting.OptionsContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Singleton
@Path("")
public class MetaEndpoint {
    final String version;
    final OptionsContext oc;

    @Inject
    public MetaEndpoint(@Named("version") String version, OptionsContext oc) {
        this.version = version;
        this.oc = oc;
    }

    @GET
    @Path("version")
    @Produces(MediaType.APPLICATION_JSON)
    public String version() {
        return version;
    }

    @GET
    @Path("options/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> options() {
        return new ArrayList<>(Options.getAll());
    }

    @GET
    @Path("options")
    @Produces(MediaType.APPLICATION_JSON)
    public OptionsInfo options(@QueryParam("name") String name) {
        if (Arrays.stream(Options.values()).map(Enum::name).anyMatch(e -> e.equals(name))) {
            return new OptionsInfo(Options.valueOf(name), oc.getOption(name));
        }

        return null;
    }
}
